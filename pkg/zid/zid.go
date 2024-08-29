package zid

import (
	"database/sql/driver"
	"fmt"
	"github.com/rs/xid"
	"strconv"
	"strings"
	"time"
)

type ID struct {
	internal xid.ID
}

func (id ID) Time() time.Time {
	return id.internal.Time()
}

func (id ID) Value() (driver.Value, error) {
	return id.internal.Value()
}

// Scan implements the sql.Scanner interface.
func (id *ID) Scan(value interface{}) (err error) {
	return id.Scan(value)
}

func (id *ID) UnmarshalText(text []byte) error {
	return id.internal.UnmarshalText(text)
}
func (id ID) MarshalText() ([]byte, error) {
	return id.internal.MarshalText()
}

func (id *ID) UnmarshalJSON(b []byte) error {
	return id.internal.UnmarshalJSON(b)
}
func (id ID) MarshalJSON() ([]byte, error) {
	return id.internal.MarshalJSON()
}

func (id ID) String() string {
	return id.internal.String()
}
func FromString(id string) (ID, error) {
	i, err := xid.FromString(id)
	if err != nil {
		return ID{}, err
	}
	return ID{internal: i}, nil
}

const tidSep = "-"

func (id ID) ToTID(transaction int) string {
	return fmt.Sprintf("%s%s%d", id.internal, tidSep, transaction)
}
func FromTID(tid string) (ID, int, error) {

	i, t, found := strings.Cut(tid, tidSep)
	if !found {
		return ID{}, 0, fmt.Errorf("could not find separator %s in %s", tidSep, tid)
	}
	tt, err := strconv.Atoi(t)
	if err != nil {
		return ID{}, 0, fmt.Errorf("could not parse tid int %s in %s, %w", t, tid, err)
	}
	id, err := FromString(i)
	if err != nil {
		return ID{}, 0, fmt.Errorf("could not parse id %s, err %w", i, err)
	}

	return id, tt, nil
}

func From(tid_eid string) (ID, error) {
	id, _, err := FromTID(tid_eid)
	if err == nil {
		return id, nil
	}
	return FromString(tid_eid)

}

func New() ID {
	return ID{
		internal: xid.New(),
	}
}
