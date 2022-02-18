# Brev
**A cli that send email directly to the mx servers of the world**

## Install
```bash
go get github.com/modfin/brev/cmd/brev
go get github.com/modfin/brev/cmd/brevd
```

## Brevd Usage
The Brevd is server for transactional emails via http api.

### Configure
* Choose a host domain you are going to use for the brevd server, eg. `brev.cc`
* Choose a client domain from which to send emails, eg. `example.com`
* Find out what ip you are running the service on, eg `1.2.3.4`
* Generate a dkim key-pair, eg `brev gen-dkim-keys --key-size=2048`. This generates two files. 
`dkim-private.pem` is loaded into `brevd` and `dkim-pub.dns.txt` which is put into a `TXT` record on
the client domain

#### Host DNS Config

Add a `A` record for your server, eg `mx0.brev.cc` pointing to the ip, `1.2.3.4`\
Should result in the following
```bash 
dig A mx0.brev.cc

;; ANSWER SECTION:
mx0.brev.cc.            260     IN      A       1.2.3.4
```

Add a `MX` record for your server to for your domain, eg `brev.cc` pointing to the brevd server\
Should result in the following
```bash 
dig MX brev.cc
 
;; ANSWER SECTION:
brev.cc.                300     IN      MX      10 mx0.brev.cc.
```

Add a SPF policy through `TXT` record for your domain, 
eg `brev.cc` pointing to you brevd server, eg `v=spf1 mx -all`\
Should result in the following
```bash 
dig TXT brev.cc 
brev.cc.                300     IN      TXT     "v=spf1 mx -all"
```

Finally, set up PTR records through your ISP, eg 1.2.3.4 -> mx0.brev.cc 

#### Client DNS Config
Say you now want send your emails and have them come from `no-reply@example.com`

Add a SPF policy to `example.com` through `TXT` record pointing to the 
`brev.cc` domain, eg `v=spf1 include:brev.cc -all`\
Should result in the following
```bash 
dig TXT example.com
 
;; ANSWER SECTION:
example.com.              60      IN      TXT     "v=spf1 include:brev.cc -all"
```

Add a DKIM public key to `example.com` by choosing a selector, eg `brev` and adding the 
public key to `TXT` record at `brev._domainkey.example.com`\
Should result in the following
```bash
dig TXT brev._domainkey.example.com

;; ANSWER SECTION:
brev._domainkey.example.com. 300  IN      TXT     "v=DKIM1; k=rsa; p=MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8A....;"
```


### Server config
Create a run script that configures the environment for you

```bash 
cat ./run-brevd.sh
export BREV_HOSTNAME=mx0.brev.cc
export BREV_DB_URI=$(pwd)/brev.sqlite

export BREV_MX_DOMAIN=brev.cc
export BREV_MX_PORT=25

export BREV_DKIM_SELECTOR=brev
export BREV_DKIM_PRIVATE_KEY=./path/to/dkim-private.pem
brevd
```





## Brev Usage
The Brev cli is a small utility to send emails directly to a MX server
```text
NAME:
   brev - a cli that send email directly to the mx servers

USAGE:
   brev [global options] command [command options] [arguments...]

COMMANDS:
   help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --subject value     Set subject line
   --from value        Set from email
   --message-id value  set the message id
   --to value          Set to email
   --cc value          Set cc email
   --bcc value         Set cc email
   --text value        text content of the mail
   --html value        html content of the mail
   --attach value      path to file attachment
   --help, -h          show help (default: false)
```


### Example

```bash 
brev --subject="An attachment" \
        --to="someone@example.com" \
        --cc="someone-else@example.com" \
        --cc="someone-compleatly-different@example.com" \
        --attach="/path/to/a.file" \
        --text='The body of the email' 
```