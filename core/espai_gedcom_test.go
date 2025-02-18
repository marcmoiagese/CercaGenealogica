package core

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestParseGEDCOMLarge(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "large.ged")

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("no puc crear fitxer GEDCOM: %v", err)
	}
	defer f.Close()

	writer := bufio.NewWriter(f)
	_, _ = fmt.Fprintln(writer, "0 HEAD")
	_, _ = fmt.Fprintln(writer, "1 SOUR CercaGenealogica")
	_, _ = fmt.Fprintln(writer, "1 GEDC")

	const totalPersons = 5000
	const totalFamilies = 2500

	for i := 1; i <= totalPersons; i++ {
		_, _ = fmt.Fprintf(writer, "0 @I%d@ INDI\n1 NAME Persona%d /Cognom%d/\n1 SEX M\n1 BIRT\n2 DATE 1 JAN 1900\n", i, i, i)
	}

	for i := 1; i <= totalFamilies; i++ {
		husb := i*2 - 1
		wife := i * 2
		child := i*2 + 1
		if child > totalPersons {
			child = 1
		}
		_, _ = fmt.Fprintf(writer, "0 @F%d@ FAM\n1 HUSB @I%d@\n1 WIFE @I%d@\n1 CHIL @I%d@\n", i, husb, wife, child)
	}

	_, _ = fmt.Fprintln(writer, "0 TRLR")
	if err := writer.Flush(); err != nil {
		t.Fatalf("no puc escriure fitxer GEDCOM: %v", err)
	}

	result, err := parseGEDCOMFile(path)
	if err != nil {
		t.Fatalf("parseGEDCOMFile ha fallat: %v", err)
	}

	if got := len(result.Persons); got != totalPersons {
		t.Fatalf("esperava %d persones, he rebut %d", totalPersons, got)
	}
	if got := len(result.Families); got != totalFamilies {
		t.Fatalf("esperava %d families, he rebut %d", totalFamilies, got)
	}
}
