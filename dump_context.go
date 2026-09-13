//go:build ignore

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var reMultipleNewlines = regexp.MustCompile(`\n{3,}`)

func scanDirectory(rootPath string) (string, error) {
	ignoreDirs := map[string]bool{
		".git": true, ".idea": true, ".llm_context": true,
		"node_modules": true, "dist": true, "bin": true, "build": true,
	}

	// Usunięto ".json": true, aby pomijać pliki JSON
	validExts := map[string]bool{
		".go": true, ".html": true, ".css": true, ".js": true, ".json": true,
	}

	var sb strings.Builder
	fileCount := 0

	err := filepath.WalkDir(rootPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			if ignoreDirs[d.Name()] {
				return filepath.SkipDir
			}
			return nil
		}

		ext := filepath.Ext(path)
		if !validExts[ext] {
			return nil
		}

		info, err := d.Info()
		if err == nil && info.Size() > 512*1024 {
			return nil
		}

		contentBytes, err := os.ReadFile(path)
		if err != nil {
			return nil
		}

		cleaned := reMultipleNewlines.ReplaceAllString(string(contentBytes), "\n\n")
		sb.WriteString(fmt.Sprintf("### %s\n```%s\n%s\n```\n\n", filepath.ToSlash(path), strings.TrimPrefix(ext, "."), strings.TrimSpace(cleaned)))
		fileCount++
		return nil
	})

	fmt.Printf("Przeskanowano: dodano %d plików.\n", fileCount)
	return sb.String(), err
}

func main() {
	flag.Parse()
	outDir := "./.llm_context"
	_ = os.MkdirAll(outDir, os.ModePerm)
	timestamp := time.Now().Format("2006-01-02_15-04-05")

	content, _ := scanDirectory(".")
	outPath := filepath.Join(outDir, fmt.Sprintf("context_full_%s.md", timestamp))
	_ = os.WriteFile(outPath, []byte(content), 0644)
	fmt.Printf("Wygenerowano plik kontekstu: %s\n", outPath)
}
