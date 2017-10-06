package main

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
)

var (
	file_error            *os.File
	count_record          int64
	count_file            int64
)

func main() {
	{
		f, err := os.Create("error.sql")

		if err != nil {
			log.Fatal("Aborting error: ", err)
		}
		file_error = f
	}

	path := "."
	var last error
	if err := processPath(path); err != nil {
		log.Printf("error procesing %q: %v", path, err)
		last = err
	}

	fmt.Fprintf(file_error, "commit;\n")
	defer file_error.Close()

	if last != nil {

		log.Print("last != nil")
		//		os.Exit(1)
	}
}

func processPath(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()

	if st, err := dir.Stat(); err != nil {
		return err
	} else if !st.IsDir() {
		return fmt.Errorf("expected a directory")
	}
	
	for {
		list, err := dir.Readdir(100)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		for _, ent := range list {
			name := ent.Name()
			fname := filepath.Join(path, name)
			is_error_sql := strings.HasPrefix(name, "error-")
			if !strings.HasSuffix(fname, ".tar.gz") {
				continue
			}
			if err := processArchive(fname, is_error_sql); err != nil {
				fmt.Println("Error! processArchive: ", fname, err)
				return err
			}
			err = os.Remove(fname)
			if err != nil {
				fmt.Println("Error remove: ", fname, err)
			}
		}
	}
	return nil
}

func processArchive(path string, is_error_sql bool) error {
	count_file++
	fmt.Println("processing: ", count_file, path)
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	zr, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer zr.Close()

	arch := tar.NewReader(zr)
	for {
		h, err := arch.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		} else if !strings.HasSuffix(h.Name, ".json") {
			continue
		}
		if err := processFile(arch, is_error_sql, h.Name); err != nil {
			return err
		}
	}
	return nil
}

func processFile(r io.Reader, is_error_sql bool, n string) error {
	s := strings.Split(n, "-") // TODO opt
	date := s[3] + "." + s[2] + "." + s[1] + " " + s[4] + ":" + s[5] + ":" + s[6]
	dec := json.NewDecoder(r)
	ip := "" //TODO GeoIP
	return processError(dec, ip, date)
}

type ErrorStats struct {
	CID string 
	Client string 
	Current  string 
	Error string `json:"error"`
}

func processError(dec *json.Decoder, ip string, date string) error {
	
	var	e ErrorStats
	if err := dec.Decode(&e); err != nil {
		return err
	}
	if err := importStat(e, ip, date); err != nil {
		return err
	}
	return nil
}
func importStat(o interface{}, ip string, date string) error {
	switch o := o.(type) {
	case ErrorStats:
		if strings.Contains(o.Error,"Media") {
		count_record++
		fmt.Fprintf(file_error, "insert into t_error values('%s','%s','%s','%s','%s','%s');\n", date, ip, o.CID, o.Client, o.Current, o.Error)
		if count_record%1000 == 0 {
			fmt.Fprintf(file_error, "commit;\n")
		}
       }
	default:
		log.Fatal("Error type: %+v\n", o)
	}
	return nil
}
