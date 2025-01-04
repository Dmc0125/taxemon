package walletshandler

import (
	"database/sql"
	"fmt"
	"html/template"
	"net/http"

	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v4"
	"github.com/mr-tron/base58"
)

func get(c echo.Context, t *template.Template) error {
	return t.ExecuteTemplate(c.Response().Writer, "wallet.html", struct{}{})
}

func post(c echo.Context, db *sqlx.DB) error {
	req := c.Request()
	err := req.ParseForm()
	if err != nil {
		return c.String(http.StatusUnprocessableEntity, "unable to parse form data")
	}

	label := req.FormValue("label")
	address := req.FormValue("address")

	bytes, err := base58.Decode(address)
	if err != nil {
		return c.String(http.StatusUnprocessableEntity, "invalid public key")
	}
	if len(bytes) != 32 {
		return c.String(http.StatusUnprocessableEntity, "invalid public key length")
	}

	labelParam := sql.NullString{}
	if len(label) != 0 {
		labelParam.Valid = true
		labelParam.String = label
	}

	// todo: should be tx
	insertedWallet := struct{ Id int32 }{}
	err = db.Get(&insertedWallet, "INSERT INTO wallet (address, label) VALUES ($1, $2) RETURNING id", address, labelParam)
	// handle wallet exists error
	fmt.Printf("wallet %#v\n", insertedWallet)

	if err != nil {
		return c.String(http.StatusInternalServerError, "unable to insert wallet")
	}
	_, err = db.Exec("INSERT INTO sync_request (wallet_id) VALUES ($1)", insertedWallet.Id)
	if err != nil {
		return c.String(http.StatusInternalServerError, "unable to insert sync request")
	}

	return c.String(http.StatusOK, "")
}

func Register(e *echo.Echo, db *sqlx.DB, t *template.Template) {
	e.GET("/wallets", func(c echo.Context) error {
		fmt.Print("HELLO")
		return get(c, t)
	})
	e.POST("/wallets", func(c echo.Context) error {
		return post(c, db)
	})
}
