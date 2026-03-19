package connector

import (
	"context"
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// MongoConnector implements the Connector interface for MongoDB.
// Top-level document fields are mapped directly to columns; nested documents
// are flattened using "parent.child" dot-notation keys.
type MongoConnector struct {
	client   *mongo.Client
	database string
}

// NewMongoConnector establishes a MongoDB connection.
// uri format: mongodb://user:pass@host:port/authdb
func NewMongoConnector(uri, database string) (*MongoConnector, error) {
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	return &MongoConnector{client: client, database: database}, nil
}

func (c *MongoConnector) DBType() string { return "mongodb" }

func (c *MongoConnector) Ping(ctx context.Context) error {
	return c.client.Ping(ctx, readpref.Primary())
}

func (c *MongoConnector) Close() error {
	return c.client.Disconnect(context.Background())
}

func (c *MongoConnector) ListTables(ctx context.Context) ([]string, error) {
	return c.client.Database(c.database).ListCollectionNames(ctx, bson.D{})
}

// GetSchema samples up to 20 documents to infer field types.
func (c *MongoConnector) GetSchema(ctx context.Context, table string) (*Schema, error) {
	coll := c.client.Database(c.database).Collection(table)
	cursor, err := coll.Find(ctx, bson.D{}, options.Find().SetLimit(20))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	fieldTypes := make(map[string]string)
	var fieldOrder []string

	for cursor.Next(ctx) {
		var doc bson.D
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		for k, v := range flattenBSONDoc(doc, "") {
			if _, exists := fieldTypes[k]; !exists {
				fieldOrder = append(fieldOrder, k)
				fieldTypes[k] = inferBSONType(v)
			}
		}
	}
	if err := cursor.Err(); err != nil {
		return nil, err
	}

	cols := make([]ColumnInfo, 0, len(fieldOrder))
	for _, name := range fieldOrder {
		cols = append(cols, ColumnInfo{
			Name:      name,
			Type:      fieldTypes[name],
			Nullable:  name != "_id",
			IsPrimary: name == "_id",
		})
	}
	return &Schema{TableName: table, Columns: cols}, nil
}

func (c *MongoConnector) CountRows(ctx context.Context, table, where string) (int64, error) {
	filter, err := parseMongoFilter(where)
	if err != nil {
		return 0, err
	}
	return c.client.Database(c.database).Collection(table).CountDocuments(ctx, filter)
}

func (c *MongoConnector) ReadBatch(ctx context.Context, opts ReadOptions) ([]Row, error) {
	filter, err := parseMongoFilter(opts.Where)
	if err != nil {
		return nil, err
	}

	findOpts := options.Find().SetSkip(opts.Offset).SetLimit(opts.Limit)
	if len(opts.Columns) > 0 {
		hasID := false
		for _, col := range opts.Columns {
			if col == "_id" {
				hasID = true
				break
			}
		}
		proj := bson.D{}
		for _, col := range opts.Columns {
			proj = append(proj, bson.E{Key: col, Value: 1})
		}
		if !hasID {
			proj = append(proj, bson.E{Key: "_id", Value: 0}) // 不请求 _id 时显式排除
		}
		findOpts.SetProjection(proj)
	}

	cursor, err := c.client.Database(c.database).Collection(opts.Table).Find(ctx, filter, findOpts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var rows []Row
	for cursor.Next(ctx) {
		var doc bson.D
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}
		rows = append(rows, flattenBSONDoc(doc, ""))
	}
	return rows, cursor.Err()
}

func (c *MongoConnector) WriteBatch(ctx context.Context, opts WriteOptions) error {
	if len(opts.Rows) == 0 {
		return nil
	}
	coll := c.client.Database(c.database).Collection(opts.Table)

	if opts.Strategy == StrategyUpsert && len(opts.PKCols) > 0 {
		models := make([]mongo.WriteModel, len(opts.Rows))
		for i, row := range opts.Rows {
			filter := bson.D{}
			for _, pk := range opts.PKCols {
				filter = append(filter, bson.E{Key: pk, Value: row[pk]})
			}
			models[i] = mongo.NewReplaceOneModel().
				SetFilter(filter).
				SetReplacement(rowToBSONDoc(row)).
				SetUpsert(true)
		}
		_, err := coll.BulkWrite(ctx, models)
		return err
	}

	docs := make([]bson.D, len(opts.Rows))
	for i, row := range opts.Rows {
		docs[i] = rowToBSONDoc(row)
	}
	_, err := coll.InsertMany(ctx, docs)
	return err
}

// flattenBSONDoc flattens a bson.D into a Row, using "parent.child" dot-notation
// for nested documents.
func flattenBSONDoc(doc bson.D, prefix string) Row {
	result := make(Row)
	for _, elem := range doc {
		key := elem.Key
		if prefix != "" {
			key = prefix + "." + key
		}
		if nested, ok := elem.Value.(bson.D); ok {
			for k2, v2 := range flattenBSONDoc(nested, key) {
				result[k2] = v2
			}
		} else {
			result[key] = elem.Value
		}
	}
	return result
}

func rowToBSONDoc(row Row) bson.D {
	doc := make(bson.D, 0, len(row))
	for k, v := range row {
		doc = append(doc, bson.E{Key: k, Value: v})
	}
	return doc
}

func inferBSONType(v any) string {
	switch v.(type) {
	case int32, int64:
		return "int"
	case float64:
		return "double"
	case bool:
		return "bool"
	default:
		return "string"
	}
}

// parseMongoFilter parses a JSON string into a bson.D filter.
// An empty string returns an empty filter (match all documents).
func parseMongoFilter(where string) (bson.D, error) {
	if strings.TrimSpace(where) == "" {
		return bson.D{}, nil
	}
	var filter bson.D
	if err := bson.UnmarshalExtJSON([]byte(where), true, &filter); err != nil {
		return nil, fmt.Errorf("invalid mongo filter JSON: %w", err)
	}
	return filter, nil
}
