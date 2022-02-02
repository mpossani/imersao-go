package repository

import (
	"database/sql"
	"github.com/mpossani/imersao-go/entity"		// go get github.com/mpossani/imersao-go/entity
)

type CourseMySQLRepository struct {
	Db *sql.DB
}

func (c CourseMySQLRepository) Insert(course entity.Course) error {
	stmt, err := c.Db.Prepare(`Insert into courses(id, name, description, status) values (?,?,?,?)`)
	if err != nil {
		return err
	}

	_, err = stmt.Exec(
		course.ID, 
		course.Name, 
		course.Description, 
		course.Status,
	)
	if err != nil {
		return err
	}

	return nil
}