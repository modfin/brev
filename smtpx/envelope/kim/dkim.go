package kim

import (
	"bytes"
	"fmt"
	"github.com/emersion/go-msgauth/dkim"
	"io"
)

type SignOptions dkim.SignOptions

type Signer struct {
	options *SignOptions
}

func New(options *SignOptions) (*Signer, error) {
	return &Signer{options: options}, nil
}

func (s *Signer) Sign(out io.Writer, in io.Reader) error {
	return dkim.Sign(out, in, (*dkim.SignOptions)(s.options))
}

func (s *Signer) Sign2(out io.Writer, in io.Reader) error {
	defer func() {
		fmt.Println("sig2 9")
	}()

	ss, err := dkim.NewSigner((*dkim.SignOptions)(s.options))
	if err != nil {
		return err
	}
	defer ss.Close()

	fmt.Println("sig2 1")
	// We need to keep the message in a buffer so we can write the new DKIM
	// header field before the rest of the message
	var b bytes.Buffer
	mw := io.MultiWriter(&b, ss)
	fmt.Println("sig2 2")
	if _, err := io.Copy(mw, in); err != nil {
		return err
	}
	fmt.Println("sig2 3")
	if err := ss.Close(); err != nil {
		return err
	}
	fmt.Println("sig2 4")
	sig := ss.Signature()
	fmt.Println("SIG", sig)
	if _, err := io.WriteString(out, sig); err != nil {
		return err
	}
	fmt.Println("sig2 5")
	_, err = io.Copy(out, &b)
	fmt.Println("sig2 6")
	return err
}

//

//func (s *Signer) Sign2(in io.Reader) ([]byte, error) {
//	b := bytes.Buffer{}
//	err := s.Sign(&b, in)
//	return b.Bytes(), err
//}
