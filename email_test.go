package brev

import (
	"fmt"
	"github.com/flashmob/go-guerrilla/mail/rfc5321"
	"testing"
)

func TestAddressIP(t *testing.T) {
	var email string
	email = "john.doe@192.0.2.1"
	addr, err := ParseAddress(email)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("name", addr.Name)
	fmt.Println("addr", addr.Address)

	var a rfc5321.RFC5322
	l, err := a.Address([]byte("john.doe@[192.0.2.1]"))
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range l.List {
		fmt.Printf("%+v\n", i)

	}

}
