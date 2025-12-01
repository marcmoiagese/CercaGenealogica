package core

import (
	"github.com/marcmoiagese/CercaGenealogica/db"
)

// App encapsula dependències compartides per evitar reobrir recursos per petició.
type App struct {
	Config map[string]string
	DB     db.DB
}

func NewApp(cfg map[string]string, database db.DB) *App {
	return &App{
		Config: cfg,
		DB:     database,
	}
}

func (a *App) Close() {
	if a.DB != nil {
		a.DB.Close()
	}
}
