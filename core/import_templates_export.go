package core

import (
	"archive/zip"
	"bytes"
	"encoding/csv"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func (a *App) importTemplateExportCSV(w http.ResponseWriter, r *http.Request, user *db.User, id int) {
	template, err := a.DB.GetCSVImportTemplate(id)
	if err != nil || template == nil {
		http.NotFound(w, r)
		return
	}
	perms, _ := a.permissionsFromContext(r)
	if !canViewImportTemplate(user, perms, template) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	model, err := parseTemplateImportModel(template.ModelJSON)
	if err != nil {
		http.Error(w, "model invalid", http.StatusBadRequest)
		return
	}
	if err := validateTemplateImportModel(model); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	headers, rows := buildTemplateExportData(model)
	sep := parseCSVSeparator(strings.TrimSpace(r.URL.Query().Get("sep")))
	if sep == 0 {
		sep = parseCSVSeparator(template.DefaultSeparator)
	}
	if sep == 0 {
		sep = ','
	}
	filename := templateExportFileName(template, "csv")
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", filename))
	writer := csv.NewWriter(w)
	writer.Comma = sep
	_ = writer.Write(headers)
	for _, row := range rows {
		_ = writer.Write(row)
	}
	writer.Flush()
}

func (a *App) importTemplateExportXLSX(w http.ResponseWriter, r *http.Request, user *db.User, id int) {
	template, err := a.DB.GetCSVImportTemplate(id)
	if err != nil || template == nil {
		http.NotFound(w, r)
		return
	}
	perms, _ := a.permissionsFromContext(r)
	if !canViewImportTemplate(user, perms, template) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	model, err := parseTemplateImportModel(template.ModelJSON)
	if err != nil {
		http.Error(w, "model invalid", http.StatusBadRequest)
		return
	}
	if err := validateTemplateImportModel(model); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	headers, rows := buildTemplateExportData(model)
	content, err := buildTemplateXLSX(headers, rows)
	if err != nil {
		http.Error(w, "failed to build xlsx", http.StatusInternalServerError)
		return
	}
	filename := templateExportFileName(template, "xlsx")
	w.Header().Set("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", filename))
	_, _ = w.Write(content)
}

func buildTemplateExportData(model *templateImportModel) ([]string, [][]string) {
	headers := make([]string, 0, len(model.Mapping))
	hasCondition := false
	for _, col := range model.Mapping {
		headers = append(headers, col.Header)
		if col.Condition != nil {
			hasCondition = true
		}
	}
	rowsCount := 2
	if hasCondition {
		rowsCount = 3
	}
	rows := make([][]string, rowsCount)
	for i := 0; i < rowsCount; i++ {
		rows[i] = make([]string, 0, len(headers))
	}
	for _, col := range model.Mapping {
		baseSample := sampleForColumn(col, 0)
		if col.Condition != nil {
			rows[0] = append(rows[0], sampleForConditionBranch(col.Condition.Then, 0))
			if col.Condition.Else != nil {
				rows[1] = append(rows[1], sampleForConditionBranch(*col.Condition.Else, 1))
			} else {
				rows[1] = append(rows[1], baseSample)
			}
			if rowsCount > 2 {
				rows[2] = append(rows[2], baseSample)
			}
		} else {
			for i := 0; i < rowsCount; i++ {
				rows[i] = append(rows[i], sampleForColumn(col, i))
			}
		}
	}
	return headers, rows
}

func sampleForConditionBranch(branch templateBranch, idx int) string {
	if len(branch.MapTo) > 0 {
		entry := branch.MapTo[0]
		return sampleFromTransforms(entry.Transforms, idx, entry.Target, "")
	}
	return sampleFromTransforms(branch.Transforms, idx, "", "")
}

func sampleForColumn(col templateColumn, idx int) string {
	if len(col.MapTo) > 0 {
		entry := col.MapTo[0]
		return sampleFromTransforms(entry.Transforms, idx, entry.Target, col.Header)
	}
	return sampleFromTransforms(nil, idx, "", col.Header)
}

func sampleFromTransforms(transforms []templateTransform, idx int, target string, header string) string {
	nameSet := map[string]bool{}
	for _, tr := range transforms {
		if name := strings.ToLower(strings.TrimSpace(tr.Name)); name != "" {
			nameSet[name] = true
		}
	}
	if nameSet["parse_ddmmyyyy_to_iso"] || nameSet["parse_date_flexible_to_base_data_acte"] || nameSet["parse_date_flexible_to_date_or_text_with_quality"] {
		if idx%3 == 1 {
			return "??/??/1890"
		}
		if idx%3 == 2 {
			return "¿12/03/1890"
		}
		return "12/03/1890"
	}
	if nameSet["parse_person_from_cognoms"] || nameSet["parse_person_from_nom"] || nameSet["parse_person_from_cognoms_marcmoia_v2"] || nameSet["parse_person_from_nom_marcmoia_v2"] {
		if idx%2 == 1 {
			return "¿Maria Puig (Valls)"
		}
		return "Puig i Ferrer (Valls)"
	}
	if nameSet["split_couple_i"] {
		return "Joan X i Maria Y"
	}
	if nameSet["normalize_cronologia"] {
		return "1890-1891"
	}
	if strings.Contains(target, "llibre_id") {
		return strconv.Itoa(120 + idx)
	}
	if strings.Contains(target, "tipus_acte") {
		return "baptisme"
	}
	if strings.Contains(target, "data") {
		return "01/01/1890"
	}
	if strings.Contains(target, "cognom") {
		return "Puig"
	}
	if strings.Contains(target, "nom") {
		return "Joan"
	}
	if strings.Contains(strings.ToLower(header), "data") {
		return "01/01/1890"
	}
	return fmt.Sprintf("Exemple %d", idx+1)
}

func templateExportFileName(template *db.CSVImportTemplate, ext string) string {
	name := strings.TrimSpace(template.Name)
	if name == "" {
		name = "plantilla"
	}
	var out strings.Builder
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			out.WriteRune(r)
		} else if r == ' ' {
			out.WriteRune('_')
		}
	}
	slug := out.String()
	if slug == "" {
		slug = "plantilla"
	}
	return slug + "." + ext
}

func buildTemplateXLSX(headers []string, rows [][]string) ([]byte, error) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	if err := addZipFile(zw, "[Content_Types].xml", buildXLSXContentTypes()); err != nil {
		return nil, err
	}
	if err := addZipFile(zw, "_rels/.rels", buildXLSXRels()); err != nil {
		return nil, err
	}
	if err := addZipFile(zw, "xl/workbook.xml", buildXLSXWorkbook()); err != nil {
		return nil, err
	}
	if err := addZipFile(zw, "xl/_rels/workbook.xml.rels", buildXLSXWorkbookRels()); err != nil {
		return nil, err
	}
	if err := addZipFile(zw, "xl/worksheets/sheet1.xml", buildXLSXSheet(headers, rows)); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func addZipFile(zw *zip.Writer, name string, content string) error {
	w, err := zw.Create(name)
	if err != nil {
		return err
	}
	_, err = w.Write([]byte(content))
	return err
}

func buildXLSXContentTypes() string {
	return `<?xml version="1.0" encoding="UTF-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
</Types>`
}

func buildXLSXRels() string {
	return `<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>`
}

func buildXLSXWorkbook() string {
	return `<?xml version="1.0" encoding="UTF-8"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
    <sheet name="Plantilla" sheetId="1" r:id="rId1"/>
  </sheets>
</workbook>`
}

func buildXLSXWorkbookRels() string {
	return `<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
</Relationships>`
}

func buildXLSXSheet(headers []string, rows [][]string) string {
	maxCols := len(headers)
	if maxCols == 0 {
		maxCols = 1
	}
	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = len([]rune(h))
	}
	for _, row := range rows {
		for i, cell := range row {
			if i >= len(widths) {
				continue
			}
			if l := len([]rune(cell)); l > widths[i] {
				widths[i] = l
			}
		}
	}
	for i, w := range widths {
		w += 2
		if w < 10 {
			w = 10
		}
		if w > 40 {
			w = 40
		}
		widths[i] = w
	}
	var cols strings.Builder
	if len(widths) > 0 {
		cols.WriteString("<cols>")
		for i, w := range widths {
			idx := i + 1
			fmt.Fprintf(&cols, `<col min="%d" max="%d" width="%d" customWidth="1"/>`, idx, idx, w)
		}
		cols.WriteString("</cols>")
	}
	var sheetData strings.Builder
	sheetData.WriteString("<sheetData>")
	writeXLSXRow(&sheetData, 1, headers)
	for i, row := range rows {
		writeXLSXRow(&sheetData, i+2, row)
	}
	sheetData.WriteString("</sheetData>")
	lastCol := columnName(maxCols)
	autoFilter := fmt.Sprintf(`<autoFilter ref="A1:%s1"/>`, lastCol)
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetViews>
    <sheetView workbookViewId="0">
      <pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/>
    </sheetView>
  </sheetViews>
  <sheetFormatPr defaultRowHeight="15"/>
  %s
  %s
  %s
</worksheet>`, cols.String(), sheetData.String(), autoFilter)
}

func writeXLSXRow(buf *strings.Builder, rowNum int, values []string) {
	buf.WriteString(fmt.Sprintf(`<row r="%d">`, rowNum))
	for i, val := range values {
		cellRef := fmt.Sprintf("%s%d", columnName(i+1), rowNum)
		escaped := escapeXML(val)
		buf.WriteString(fmt.Sprintf(`<c r="%s" t="inlineStr"><is><t>%s</t></is></c>`, cellRef, escaped))
	}
	buf.WriteString("</row>")
}

func columnName(index int) string {
	if index <= 0 {
		return "A"
	}
	name := ""
	for index > 0 {
		index--
		name = string(rune('A'+(index%26))) + name
		index /= 26
	}
	return name
}

func escapeXML(val string) string {
	replacer := strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		"\"", "&quot;",
		"'", "&apos;",
	)
	return replacer.Replace(val)
}
