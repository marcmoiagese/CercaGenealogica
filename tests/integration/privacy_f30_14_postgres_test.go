package integration

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
	testcommon "github.com/marcmoiagese/CercaGenealogica/tests/common"
)

func TestCreatePrivacyDefaultsPersistsBooleanFlagsF3014Emergency(t *testing.T) {
	dbConfs := testcommon.LoadTestDBConfigs(t)

	for _, dbCfg := range dbConfs {
		dbCfg := dbCfg

		t.Run(dbCfg.Label, func(t *testing.T) {
			if strings.ToLower(dbCfg.Engine) == "mysql" {
				t.Skip("motor mysql omès en aquest entorn per incidència externa d'inicialització")
			}

			cfg := newConfigForDB(t, dbCfg, "test_f30_14_privacy_defaults.sqlite3")
			_, database := newTestAppForConfig(t, cfg)

			email := fmt.Sprintf("f30_14_privacy_%s@example.com", dbCfg.Engine)
			username := fmt.Sprintf("f30_14_privacy_%s", dbCfg.Engine)
			testcommon.CleanupUser(t, database, dbCfg.Engine, username, email)

			user := &db.User{
				Usuari:        username,
				Name:          "Privacy",
				Surname:       "Defaults",
				Email:         email,
				Password:      []byte("hash_dummy"),
				DataNaixament: "1990-01-01",
				Active:        false,
				CreatedAt:     time.Now().Format(time.RFC3339),
			}
			if err := database.InsertUser(user); err != nil {
				t.Fatalf("[%s] InsertUser ha fallat: %v", dbCfg.Label, err)
			}

			createdUser, err := database.GetUserByEmail(email)
			if err != nil || createdUser == nil {
				t.Fatalf("[%s] GetUserByEmail ha fallat després d'InsertUser: %v", dbCfg.Label, err)
			}

			if err := database.CreatePrivacyDefaults(createdUser.ID); err != nil {
				t.Fatalf("[%s] CreatePrivacyDefaults ha fallat: %v", dbCfg.Label, err)
			}

			privacy, err := database.GetPrivacySettings(createdUser.ID)
			if err != nil || privacy == nil {
				t.Fatalf("[%s] GetPrivacySettings ha fallat: %v", dbCfg.Label, err)
			}
			if !privacy.ShowActivity || !privacy.ProfilePublic || !privacy.NotifyEmail || !privacy.AllowContact {
				t.Fatalf("[%s] flags booleans inesperats a user_privacy: %+v", dbCfg.Label, privacy)
			}
		})
	}
}
