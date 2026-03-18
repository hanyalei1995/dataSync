package service

import (
	"datasync/internal/model"
	"errors"

	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
)

type UserService struct {
	DB *gorm.DB
}

func (s *UserService) EnsureAdmin(username, password string) error {
	var user model.User
	err := s.DB.Where("username = ?", username).First(&user).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		hash, _ := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
		return s.DB.Create(&model.User{
			Username:     username,
			PasswordHash: string(hash),
		}).Error
	}
	return err
}

func (s *UserService) Authenticate(username, password string) (*model.User, error) {
	var user model.User
	if err := s.DB.Where("username = ?", username).First(&user).Error; err != nil {
		return nil, errors.New("invalid credentials")
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)); err != nil {
		return nil, errors.New("invalid credentials")
	}
	return &user, nil
}
