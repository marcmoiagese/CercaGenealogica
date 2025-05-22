package mon

type CountryConfig struct {
	Name   string
	Fields []string // ex: ["Municipi", "Comunitat Autònoma", ...]
}

var CountryStructure = map[string][]string{
	"Espanya":         {"Municipi", "Comunitat Autònoma", "Provincia", "Comarca"},
	"França":          {"Municipi", "Regió", "Departament", "Districte"},
	"USA":             {"Municipi", "Estat", "Comtat", "No aplica"},
	"Itàlia":          {"Municipi", "Regió", "Provincia", "No aplica"},
	"Canadà":          {"Poble", "Província", "Comtat", "Municipi"},
	"Pais desconegut": {},
}
