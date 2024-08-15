package api

import (
	"fmt"
	"github.com/labstack/echo/v4"
	"github.com/modfin/brev/internal/old/dao"
	"net/http"
	"net/url"
)

func updateKeySettings(db dao.DAO) echo.HandlerFunc {
	return func(c echo.Context) error {
		key, err := getApiKey(c, db)
		if err != nil {
			return fmt.Errorf("failed to retrive key, err %v", err)
		}
		var input struct {
			Domain      string `json:"domain"`
			MxCNAME     string `json:"mx_cname"`
			PosthookURL string `json:"posthook_url"`
		}
		err = c.Bind(&input)
		if err != nil {
			return err
		}
		if _, err := url.Parse(input.PosthookURL); err != nil || input.Domain == "" || input.MxCNAME == "" {
			return echo.ErrBadRequest
		}
		key.Domain = input.Domain
		key.MxCNAME = input.MxCNAME
		key.PosthookURL = input.PosthookURL
		_, err = db.UpsertApiKey(key)
		if err != nil {
			return err
		}
		return c.NoContent(http.StatusNoContent)
	}
}
