package main

import (
	"github.com/gorilla/mux"
	"github.com/kudinovdenis/acronis-gd/acronis_admin_client"
	"github.com/kudinovdenis/acronis-gd/acronis_gmail"
	"github.com/kudinovdenis/acronis-gd/config"
	"github.com/kudinovdenis/acronis-gd/utils"
	"github.com/kudinovdenis/logger"
	"github.com/urfave/negroni"
	"google.golang.org/api/admin/directory/v1"
	"io/ioutil"
	"net/http"
	"sync"
)

const gmailTestEmail = "monica.geller@trueimage.eu"

var errors = []error{}

func GmailToGmail() {
	gb, err := acronis_gmail.Init("ao@dkudinov.com")
	if err != nil {
		logger.Logf(logger.LogLevelError, "Failed to Create service backup ao, err: %v", err.Error())
	}

	err = gb.Backup()
	if err != nil {
		logger.Logf(logger.LogLevelError, "Failed to backup, err: %v", err.Error())
	}

	gr, err := acronis_gmail.Init("to@dkudinov.com")
	if err != nil {
		logger.Logf(logger.LogLevelError, "Failed to Create service backup to, err: %v", err.Error())
	}
	err = gr.Restore("./ao@dkudinov.com/backup/")
	if err != nil {
		logger.Logf(logger.LogLevelError, "Failed to resotre, err: %v", err.Error())
	}
}

func clientHandler(rw http.ResponseWriter, r *http.Request) {
	domain := r.URL.Query().Get("domain")
	admin_email := r.URL.Query().Get("admin_email")

	if domain == "" || admin_email == "" {
		http.Error(rw, "Must provide domain and admin_email.", http.StatusBadRequest)
		return
	}

	rw.Write(
		[]byte(
			`<!doctype html>
<html>

	<head>
		<title>Admin panel</title>
		<script type="text/javascript" src="https://apis.google.com/js/platform.js"></script>
	</head>

	<body>
		<h1>Improvised Admin Panel</h1>
		<div><a href="/backup?domain=` + domain + "&amp;admin_email=" + admin_email + `">backup Drive now</a></div>
		<div><a href="/users?domain=` + domain + "&amp;admin_email=" + admin_email + `">show users</a></div>
		<div><a href="/backup_gmail">Backup Gmail now</a></div>
		<div><a href="/backup_gmail_incrementally">Backup Gmail incrementally</a></div>
		<div><a href="/restore_gmail">Restore Gmail</a></div>
		<div><a href="/backups/">browse</a></div>
		<a style="display: block; margin-top: 100px;" href="http://cs6.pikabu.ru/post_img/2015/06/09/10/1433867902_2044988577.jpg">"Не быть тебе дизайнером"</a>
		<div class="g-additnow" data-applicationid="951874456850"></div>
	</body>
</html>`))
}

func backupHandler(rw http.ResponseWriter, r *http.Request) {
	domain := r.URL.Query().Get("domain")
	admin_email := r.URL.Query().Get("admin_email")

	if domain == "" || admin_email == "" {
		http.Error(rw, "Must provide domain and admin_email.", http.StatusBadRequest)
		return
	}

	admin_client, err := acronis_admin_client.Init(domain, admin_email)
	if err != nil {
		logger.Logf(logger.LogLevelDefault, "Cant initialize admin client. %s", err.Error())
		return
	}

	users, err := admin_client.GetListOfUsers()
	if err != nil {
		logger.Logf(logger.LogLevelDefault, "Error while getting list of users: %s", err.Error())
		return
	}

	group := sync.WaitGroup{}
	group.Add(len(users.Users))

	// Google drive
	for _, user := range users.Users {
		go func(user *admin.User) {
			backupUserGoogleDrive(user)
			group.Done()
		}(user)
	}

	group.Wait()
	logger.Logf(logger.LogLevelDefault, "Errors: %d. %#v", len(errors), errors)
}

func gmailBackupHandler(writer http.ResponseWriter, request *http.Request) {
	client, err := acronis_gmail.Init(gmailTestEmail)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	err = client.Backup()
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
}

func gmailIncrementalBackupHandler(writer http.ResponseWriter, request *http.Request) {
	client, err := acronis_gmail.Init(gmailTestEmail)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	baseFolder := "./backups/gmail/" + gmailTestEmail
	err = client.BackupIncrementally(baseFolder+"/backup/", baseFolder+"/backup.json")
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
}

func gmailRestoreHandler(writer http.ResponseWriter, request *http.Request) {
	client, err := acronis_gmail.Init(gmailTestEmail)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	err = client.Restore("./backups/gmail/" + gmailTestEmail + "/backup")
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
}

func usersHandler(rw http.ResponseWriter, r *http.Request) {
	domain := r.URL.Query().Get("domain")
	admin_email := r.URL.Query().Get("admin_email")

	if domain == "" || admin_email == "" {
		http.Error(rw, "Must provide domain and admin_email.", http.StatusBadRequest)
		return
	}

	admin_client, err := acronis_admin_client.Init(domain, admin_email)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		logger.Logf(logger.LogLevelDefault, "Cant initialize admin client. %s", err.Error())
		return
	}

	users, err := admin_client.GetListOfUsers()
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		logger.Logf(logger.LogLevelDefault, "Error while getting list of users: %s", err.Error())
		return
	}

	htmlListOfUsers := "<div><ul>"
	for _, user := range users.Users {
		htmlListOfUsers += "<li>" + "ID: " + user.Id + " Name: " + user.Name.FullName + "</li>"
	}
	htmlListOfUsers += "</ul></div>"

	rw.Write(
		[]byte(
			"<h1>Improvised Admin Panel</h1>" +
				"<h2>Users</h2>" +
				htmlListOfUsers))
}

func googleDomainVerificationHandler(rw http.ResponseWriter, r *http.Request) {
	reader, err := utils.ReadFile("./google7ded6bed08ed3c1b.html")
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	b, err := ioutil.ReadAll(reader)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	rw.Write(b)
}

func main() {
	err := config.PopulateConfigWithFile("config.json")
	if err != nil {
		logger.Logf(logger.LogLevelError, "Cant read config file. %s", err.Error())
		return
	}
	logger.Logf(logger.LogLevelDefault, "Config: %#v", config.Cfg)

	n := negroni.Classic()

	r := mux.NewRouter()
	// Main admin panel
	r.HandleFunc("/client", clientHandler).Methods("GET")
	// Methods
	r.HandleFunc("/backup", backupHandler).Methods("GET")
	r.HandleFunc("/backup_gmail", gmailBackupHandler).Methods("GET")
	r.HandleFunc("/backup_gmail_incrementally", gmailIncrementalBackupHandler).Methods("GET")
	r.HandleFunc("/restore_gmail", gmailRestoreHandler).Methods("GET")
	r.HandleFunc("/users", usersHandler).Methods("GET")
	// Registration / authorization flow
	r.HandleFunc("/authorize", authorizationHandler).Methods("GET")
	r.HandleFunc("/oauth2callback", oauth2CallbackHandler).Methods("GET")
	// Google domain verification
	r.HandleFunc("/google7ded6bed08ed3c1b.html", googleDomainVerificationHandler).Methods("GET")
	// Notifications
	r.HandleFunc("/googleDriveNotifyCallback", googleDriveNotifyCallback)

	r.PathPrefix("/" + config.Cfg.BackupsDirectory + "/").Handler(
		http.StripPrefix("/"+config.Cfg.BackupsDirectory+"/", http.FileServer(http.Dir(config.Cfg.BackupsDirectory+"/"))))

	n.UseHandler(r)

	if config.Cfg.UseLocalServer {
		logger.Logf(logger.LogLevelError, "%s", http.ListenAndServe(config.Cfg.Port, n).Error())
	} else {
		logger.Logf(logger.LogLevelError, "%s", http.ListenAndServeTLS(config.Cfg.Port, "./cert.pem", "./privkey.pem", n))
	}
}

func processError(err error) {
	logger.Logf(logger.LogLevelError, "Error: %#v", err.Error())
	errors = append(errors, err)
}
