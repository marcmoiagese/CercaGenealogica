package core

import (
	"fmt"
	"html"
	"html/template"
	"regexp"
	"strings"
)

var (
	reMessageURLSimple = regexp.MustCompile(`(?is)\[url\]([^\[]+?)\[/url\]`)
	reMessageURLAttr   = regexp.MustCompile(`(?i)\[url=([^\]]+)\]`)
	reMessageSizeTag   = regexp.MustCompile(`(?i)\[size=([^\]]+)\]`)
	reMessageTag       = regexp.MustCompile(`\[[^\]]+\]`)
)

func renderMessageHTML(body string) template.HTML {
	body = strings.ReplaceAll(body, "\r\n", "\n")
	body = strings.ReplaceAll(body, "\r", "\n")
	if strings.TrimSpace(body) == "" {
		return template.HTML("")
	}
	escaped := html.EscapeString(body)
	escaped = renderMessageURLSimpleTags(escaped)
	escaped = renderMessageURLTags(escaped)
	escaped = renderMessageSizeTags(escaped)
	escaped = regexp.MustCompile(`(?i)\[/size\]`).ReplaceAllString(escaped, "</span>")
	escaped = replaceMessageTag(escaped, "b", "<strong>", "</strong>")
	escaped = replaceMessageTag(escaped, "i", "<em>", "</em>")
	escaped = replaceMessageTag(escaped, "u", "<span class=\"msg-underline\">", "</span>")
	escaped = replaceMessageTag(escaped, "indent", "<div class=\"msg-indent\">", "</div>")
	escaped = replaceMessageTag(escaped, "table", "<table class=\"msg-table\">", "</table>")
	escaped = replaceMessageTag(escaped, "tr", "<tr>", "</tr>")
	escaped = replaceMessageTag(escaped, "td", "<td>", "</td>")
	escaped = strings.ReplaceAll(escaped, "\n", "<br>")
	return template.HTML(escaped)
}

func stripMessageMarkup(body string) string {
	if strings.TrimSpace(body) == "" {
		return ""
	}
	body = reMessageTag.ReplaceAllString(body, "")
	body = strings.ReplaceAll(body, "\n", " ")
	body = strings.ReplaceAll(body, "\r", " ")
	body = strings.Join(strings.Fields(body), " ")
	return strings.TrimSpace(body)
}

func renderMessageURLSimpleTags(input string) string {
	return reMessageURLSimple.ReplaceAllStringFunc(input, func(m string) string {
		parts := reMessageURLSimple.FindStringSubmatch(m)
		if len(parts) < 2 {
			return m
		}
		url := strings.TrimSpace(parts[1])
		if !isAllowedMessageURL(url) {
			return m
		}
		return fmt.Sprintf("[url=%s]%s[/url]", url, url)
	})
}

func renderMessageURLTags(input string) string {
	replaced := reMessageURLAttr.ReplaceAllStringFunc(input, func(m string) string {
		parts := reMessageURLAttr.FindStringSubmatch(m)
		if len(parts) < 2 {
			return m
		}
		url := strings.TrimSpace(parts[1])
		if !isAllowedMessageURL(url) {
			return m
		}
		return fmt.Sprintf(`<a href="%s" class="msg-link" rel="noopener noreferrer" target="_blank">`, url)
	})
	replaced = regexp.MustCompile(`(?i)\[/url\]`).ReplaceAllString(replaced, "</a>")
	return replaced
}

func renderMessageSizeTags(input string) string {
	return reMessageSizeTag.ReplaceAllStringFunc(input, func(m string) string {
		parts := reMessageSizeTag.FindStringSubmatch(m)
		if len(parts) < 2 {
			return m
		}
		size := strings.ToLower(strings.TrimSpace(parts[1]))
		className := ""
		switch size {
		case "small", "s", "petita":
			className = "msg-size-small"
		case "normal", "m", "mitja":
			className = "msg-size-normal"
		case "large", "l", "gran":
			className = "msg-size-large"
		case "xl", "xlarge", "extra", "molt-gran":
			className = "msg-size-xl"
		}
		if className == "" {
			return m
		}
		return fmt.Sprintf(`<span class="%s">`, className)
	})
}

func replaceMessageTag(input, tag, open, close string) string {
	openRe := regexp.MustCompile(`(?i)\[` + regexp.QuoteMeta(tag) + `\]`)
	closeRe := regexp.MustCompile(`(?i)\[\/` + regexp.QuoteMeta(tag) + `\]`)
	input = openRe.ReplaceAllString(input, open)
	return closeRe.ReplaceAllString(input, close)
}

func isAllowedMessageURL(url string) bool {
	lower := strings.ToLower(url)
	return strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://")
}
