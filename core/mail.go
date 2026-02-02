package core

import (
	"bytes"
	"fmt"
	"net/smtp"
	"os/exec"
	"strings"
)

// MailConfig encapsula la configuració d'enviament de correu.
type MailConfig struct {
	Enabled  bool
	From     string
	SMTPHost string
	SMTPPort string
}

var mailSendOverride func(to, subject, body string) error

// SetMailSendOverride permet injectar un sender per tests.
func SetMailSendOverride(fn func(to, subject, body string) error) {
	mailSendOverride = fn
}

// NewMailConfig construeix una MailConfig a partir del map de configuració.
func NewMailConfig(cfg map[string]string) MailConfig {
	enabled := strings.ToLower(strings.TrimSpace(cfg["MAIL_ENABLED"])) == "true"

	from := strings.TrimSpace(cfg["MAIL_FROM"])
	if from == "" {
		from = "no-reply@localhost"
	}

	host := strings.TrimSpace(cfg["MAIL_SMTP_HOST"])
	if host == "" {
		host = "localhost"
	}

	port := strings.TrimSpace(cfg["MAIL_SMTP_PORT"])
	if port == "" {
		port = "25"
	}

	return MailConfig{
		Enabled:  enabled,
		From:     from,
		SMTPHost: host,
		SMTPPort: port,
	}
}

// Send envia un correu utilitzant primer el binari local sendmail i, si no està disponible, fa servir SMTP.
func (mc MailConfig) Send(to, subject, body string) error {
	if !mc.Enabled {
		return nil
	}
	if mailSendOverride != nil {
		return mailSendOverride(to, subject, body)
	}

	msg := buildRFC822(mc.From, to, subject, body)

	if err := mc.sendViaSendmail(msg); err == nil {
		return nil
	} else {
		Debugf("sendmail no disponible, provant SMTP: %v", err)
	}

	return mc.sendViaSMTP(msg, to)
}

func (mc MailConfig) sendViaSendmail(msg []byte) error {
	path, err := exec.LookPath("sendmail")
	if err != nil {
		return err
	}

	cmd := exec.Command(path, "-t", "-oi")
	cmd.Stdin = bytes.NewReader(msg)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		if stderr.Len() > 0 {
			return fmt.Errorf("sendmail: %w: %s", err, strings.TrimSpace(stderr.String()))
		}
		return fmt.Errorf("sendmail: %w", err)
	}

	return nil
}

func (mc MailConfig) sendViaSMTP(msg []byte, to string) error {
	addr := fmt.Sprintf("%s:%s", mc.SMTPHost, mc.SMTPPort)
	return smtp.SendMail(addr, nil, mc.From, []string{to}, msg)
}

func buildRFC822(from, to, subject, body string) []byte {
	headers := []string{
		fmt.Sprintf("From: %s", from),
		fmt.Sprintf("To: %s", to),
		fmt.Sprintf("Subject: %s", subject),
		"MIME-Version: 1.0",
		"Content-Type: text/plain; charset=UTF-8",
		"",
		body,
	}
	return []byte(strings.Join(headers, "\r\n"))
}
