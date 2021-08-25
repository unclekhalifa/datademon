package datademon

import (
	"archive/zip"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

func Unzip(src string, dest string) ([]string, error) {
	var fileNames []string

	reader, err := zip.OpenReader(src)
	if err != nil {
		return nil, err
	}

	defer func(reader *zip.ReadCloser) {
		err = reader.Close()
	}(reader)
	if err != nil {
		return nil, err
	}

	for _, f := range reader.File {
		fPath := filepath.Join(dest, f.Name)

		if !strings.HasPrefix(fPath, filepath.Clean(dest)+string(os.PathSeparator)) {
			return nil, fmt.Errorf("%s: illegal file path", fPath)
		}

		fileNames = append(fileNames, fPath)

		if f.FileInfo().IsDir() {
			err := os.MkdirAll(fPath, os.ModePerm)
			if err != nil {
				return nil, err
			}
			continue
		}

		if err = os.MkdirAll(filepath.Dir(fPath), os.ModePerm); err != nil {
			return fileNames, err
		}

		outFile, err := os.OpenFile(fPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return nil, err
		}

		rc, err := f.Open()
		if err != nil {
			return nil, err
		}

		_, err = io.Copy(outFile, rc)
		if err != nil {
			return nil, err
		}

		err = outFile.Close()
		if err != nil {
			return nil, err
		}

		err = rc.Close()
		if err != nil {
			return nil, err
		}
	}
	return fileNames, nil
}

func ReadCsvFile(file string) ([][]string, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}

	defer func(f *os.File) {
		err = f.Close()
	}(f)
	if err != nil {
		return nil, err
	}

	csvReader := csv.NewReader(f)
	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, err
	}

	return records, nil
}

func DownloadZipFile(url string, filePath string) (bool, error) {
	resp, err := http.Get(url)
	if err != nil {
		return false, err
	}

	defer func(Body io.ReadCloser) {
		err = Body.Close()
	}(resp.Body)
	if err != nil {
		return false, err
	}

	if resp.StatusCode != 200 {
		return false, fmt.Errorf("could not download file: %x", resp.StatusCode)
	}

	out, err := os.Create(filePath)
	if err != nil {
		return false, err
	}

	defer func(out *os.File) {
		err = out.Close()
	}(out)
	if err != nil {
		return false, err
	}

	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return false, err
	}

	return true, nil
}

func ParseCsv(records [][]string, callback func(int, []string) bool) {
	for i, record := range records {
		terminate := callback(i, record)
		if terminate {
			break
		}
	}
}
