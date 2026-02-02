package core

import (
	"strings"
	"testing"
)

func TestParseFlexibleDateV2Cases(t *testing.T) {
	tests := []struct {
		raw    string
		iso    string
		status string
	}{
		{"12/03/1803", "1803-03-12", "clar"},
		{"?/03/1804", "", "dubtos"},
		{"??/??/1804", "", "dubtos"},
		{"10/10/18?4", "", "dubtos"},
		{"¿", "", "no_consta"},
		{"15.03.1803", "1803-03-15", "clar"},
		{"  1/ 3 /1803 ", "1803-03-01", "clar"},
	}
	for _, tt := range tests {
		iso, text, status := parseFlexibleDateV2(tt.raw)
		if iso != tt.iso {
			t.Fatalf("parseFlexibleDateV2(%q) iso=%q, esperava %q", tt.raw, iso, tt.iso)
		}
		if status != tt.status {
			t.Fatalf("parseFlexibleDateV2(%q) status=%q, esperava %q", tt.raw, status, tt.status)
		}
		if tt.iso == "" && strings.TrimSpace(tt.raw) != "" && text == "" {
			t.Fatalf("parseFlexibleDateV2(%q) text buit quan no hi ha ISO", tt.raw)
		}
	}
}

func TestParentheticalsV2(t *testing.T) {
	person := buildPersonFromCognomsV2("Puig Joan (pagès) (Valls)", "batejat")
	if person == nil {
		t.Fatalf("buildPersonFromCognomsV2 ha retornat nil")
	}
	if person.MunicipiText != "Valls" {
		t.Fatalf("municipi esperat Valls, tinc %q", person.MunicipiText)
	}
	if !strings.Contains(person.Notes, "pagès") {
		t.Fatalf("notes esperades amb pagès, tinc %q", person.Notes)
	}
	if person.Nom != "Joan" || person.Cognom1 != "Puig" {
		t.Fatalf("nom/cognom incorrectes: nom=%q cognom1=%q", person.Nom, person.Cognom1)
	}
}

func TestMarriageOrderParsing(t *testing.T) {
	cases := []struct {
		raw   string
		want  int
		valid bool
	}{
		{"casat (2)", 2, true},
		{"2n matrimoni", 2, true},
		{"matrimoni 1: Maria", 1, true},
	}
	for _, c := range cases {
		got, ok := parseMarriageOrder(c.raw)
		if ok != c.valid {
			t.Fatalf("parseMarriageOrder(%q) ok=%v, esperava %v", c.raw, ok, c.valid)
		}
		if got != c.want {
			t.Fatalf("parseMarriageOrder(%q)=%d, esperava %d", c.raw, got, c.want)
		}
	}
}
