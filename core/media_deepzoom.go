package core

import (
	"encoding/xml"
	"errors"
	"fmt"
	"image"
	"image/draw"
	"image/jpeg"
	"math"
	"os"
	"path/filepath"
	"strings"
)

type dziImage struct {
	XMLName  xml.Name `xml:"Image"`
	Xmlns    string   `xml:"xmlns,attr"`
	TileSize int      `xml:"TileSize,attr"`
	Overlap  int      `xml:"Overlap,attr"`
	Format   string   `xml:"Format,attr"`
	Size     dziSize  `xml:"Size"`
}

type dziSize struct {
	Width  int `xml:"Width,attr"`
	Height int `xml:"Height,attr"`
}

// BuildDeepZoom genera la piràmide DeepZoom i retorna mida original i maxLevel.
func BuildDeepZoom(originalPath, outputDir string, tileSize int, format string) (int, int, int, error) {
	if tileSize <= 0 {
		tileSize = 256
	}
	format = normalizeDZIFormat(format)

	img, err := decodeImageFile(originalPath)
	if err != nil {
		return 0, 0, 0, err
	}
	bounds := img.Bounds()
	width := bounds.Dx()
	height := bounds.Dy()
	if width <= 0 || height <= 0 {
		return 0, 0, 0, errors.New("invalid image dimensions")
	}

	maxLevel := deepZoomMaxLevel(width, height)
	if err := os.RemoveAll(outputDir); err != nil {
		return 0, 0, 0, err
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return 0, 0, 0, err
	}

	for level := 0; level <= maxLevel; level++ {
		levelWidth, levelHeight := deepZoomLevelSize(width, height, maxLevel, level)
		scaled := resizeImageNearest(img, levelWidth, levelHeight)

		tilesX := int(math.Ceil(float64(levelWidth) / float64(tileSize)))
		tilesY := int(math.Ceil(float64(levelHeight) / float64(tileSize)))
		levelDir := filepath.Join(outputDir, "dz_files", fmt.Sprintf("%d", level))
		if err := os.MkdirAll(levelDir, 0o755); err != nil {
			return 0, 0, 0, err
		}

		for y := 0; y < tilesY; y++ {
			for x := 0; x < tilesX; x++ {
				tilePath := filepath.Join(levelDir, fmt.Sprintf("%d_%d.%s", x, y, format))
				if err := writeTile(scaled, tilePath, x*tileSize, y*tileSize, tileSize, tileSize, format); err != nil {
					return 0, 0, 0, err
				}
			}
		}
	}

	if err := writeDZI(filepath.Join(outputDir, "dz.dzi"), tileSize, format, width, height); err != nil {
		return 0, 0, 0, err
	}

	return width, height, maxLevel, nil
}

func decodeImageFile(path string) (image.Image, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	img, _, err := image.Decode(f)
	if err != nil {
		return nil, err
	}
	return img, nil
}

func deepZoomMaxLevel(width, height int) int {
	maxDim := width
	if height > maxDim {
		maxDim = height
	}
	if maxDim <= 1 {
		return 0
	}
	return int(math.Ceil(math.Log2(float64(maxDim))))
}

func deepZoomLevelSize(width, height, maxLevel, level int) (int, int) {
	if maxLevel <= 0 {
		return width, height
	}
	scale := math.Pow(2, float64(maxLevel-level))
	lw := int(math.Ceil(float64(width) / scale))
	lh := int(math.Ceil(float64(height) / scale))
	if lw < 1 {
		lw = 1
	}
	if lh < 1 {
		lh = 1
	}
	return lw, lh
}

func resizeImageNearest(src image.Image, newWidth, newHeight int) *image.RGBA {
	if newWidth <= 0 {
		newWidth = 1
	}
	if newHeight <= 0 {
		newHeight = 1
	}
	dst := image.NewRGBA(image.Rect(0, 0, newWidth, newHeight))
	bounds := src.Bounds()
	srcW := bounds.Dx()
	srcH := bounds.Dy()
	for y := 0; y < newHeight; y++ {
		srcY := bounds.Min.Y + int(float64(y)*float64(srcH)/float64(newHeight))
		for x := 0; x < newWidth; x++ {
			srcX := bounds.Min.X + int(float64(x)*float64(srcW)/float64(newWidth))
			dst.Set(x, y, src.At(srcX, srcY))
		}
	}
	return dst
}

func writeTile(img image.Image, tilePath string, x0, y0, tileW, tileH int, format string) error {
	bounds := img.Bounds()
	maxX := x0 + tileW
	maxY := y0 + tileH
	if maxX > bounds.Dx() {
		maxX = bounds.Dx()
	}
	if maxY > bounds.Dy() {
		maxY = bounds.Dy()
	}
	if x0 >= maxX || y0 >= maxY {
		return nil
	}

	tile := image.NewRGBA(image.Rect(0, 0, maxX-x0, maxY-y0))
	draw.Draw(tile, tile.Bounds(), img, image.Point{X: x0, Y: y0}, draw.Src)

	out, err := os.Create(tilePath)
	if err != nil {
		return err
	}
	defer out.Close()

	switch format {
	case "jpg":
		return jpeg.Encode(out, tile, &jpeg.Options{Quality: 85})
	default:
		return fmt.Errorf("unsupported tile format: %s", format)
	}
}

func writeDZI(path string, tileSize int, format string, width, height int) error {
	dzi := dziImage{
		Xmlns:    "http://schemas.microsoft.com/deepzoom/2008",
		TileSize: tileSize,
		Overlap:  0,
		Format:   format,
		Size: dziSize{
			Width:  width,
			Height: height,
		},
	}
	payload, err := xml.MarshalIndent(dzi, "", "  ")
	if err != nil {
		return err
	}
	payload = append([]byte(xml.Header), payload...)
	return os.WriteFile(path, payload, 0o644)
}

func normalizeDZIFormat(format string) string {
	format = strings.ToLower(strings.TrimSpace(format))
	format = strings.TrimPrefix(format, ".")
	if format == "jpeg" {
		format = "jpg"
	}
	if format == "" {
		format = "jpg"
	}
	return format
}
