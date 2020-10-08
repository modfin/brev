# Brev
**A cli that send email directly to the mx servers of the world**

## Install
```bash
go get github.com/crholm/brev
```


## Usage
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
brev.go --subject="An attachment" \
        --to="someone@example.com" \
        --cc="someone-else@example.com" \
        --cc="someone-compleatly-different@example.com" \
        --attach="/path/to/a.file" \
        --text='The body of the email' 
```