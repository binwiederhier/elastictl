package tools

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
)

func Reshard(host string, index string, dir string, keep bool, search string, workers int, shards int, replicas int) error {
	if dir == "" {
		var err error
		dir, err = os.Getwd()
		if err != nil {
			return err
		}
	}
	filename := filepath.Join(dir, fmt.Sprintf("%s.json", index))
	file, err := os.OpenFile(filename, os.O_CREATE | os.O_TRUNC | os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	exported, err := Export(host, index, search, file)
	if err != nil {
		return err
	}
	if _, err := file.Seek(0,0); err != nil {
		return err
	}
	lines, err := lineCounter(file)
	if err != nil {
		return err
	}
	if exported != lines-1 {
		return fmt.Errorf("unexpected count: %d documents expected in exported file, got %d", exported, lines-1)
	}
	if _, err := file.Seek(0,0); err != nil {
		return err
	}
	if err := deleteIndex(host, index); err != nil {
		return err
	}
	imported, err := Import(host, index, workers, false, shards, replicas, file, exported)
	if err != nil {
		return err
	}
	if imported != exported {
		return fmt.Errorf("count mismatch: %d documents exported, but %d imported", exported, imported)
	}
	if !keep {
		file.Close()
		os.Remove(filename)
	}
	log.Printf("resharding complete")
	return nil
}

func deleteIndex(host, index string) error {
	indexURI := fmt.Sprintf("http://%s/%s", host, index)
	req, err := http.NewRequest("DELETE", indexURI, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != 201 && resp.StatusCode != 200 {
		return fmt.Errorf("unexpected response code during delete call: %d %s\n", resp.StatusCode, string(responseBody))
	}
	return nil
}

func lineCounter(r io.Reader) (int, error) {
	// From: https://stackoverflow.com/questions/24562942/golang-how-do-i-determine-the-number-of-lines-in-a-file-efficiently
	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}
