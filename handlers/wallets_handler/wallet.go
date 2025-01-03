package walletshandler

import (
	"context"
	"fmt"
	"html/template"
	"net/http"
	"taxemon/pkg/dbgen"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/mr-tron/base58"
)

func get(c echo.Context, t *template.Template) error {
	return t.ExecuteTemplate(c.Response().Writer, "wallet.html", struct{}{})
}

func post(c echo.Context, q *dbgen.Queries) error {
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

	params := &dbgen.InsertWalletParams{
		Address: address,
	}
	if len(label) != 0 {
		params.Label.Valid = true
		params.Label.String = label
	}

	walletId, err := q.InsertWallet(context.Background(), params)
	if err != nil {
		return c.String(http.StatusInternalServerError, "unable to insert wallet")
	}
	err = q.InsertSyncRequest(context.Background(), &dbgen.InsertSyncRequestParams{
		WalletID:  walletId,
		CreatedAt: time.Now().Unix(),
	})
	if err != nil {
		return c.String(http.StatusInternalServerError, "unable to insert sync request")
	}

	return c.String(http.StatusOK, "")
}

func Register(e *echo.Echo, q *dbgen.Queries, t *template.Template) {
	e.GET("/wallets", func(c echo.Context) error {
		fmt.Print("HELLO")
		return get(c, t)
	})
	e.POST("/wallets", func(c echo.Context) error {
		return post(c, q)
	})
}
