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
	file_torrent, file_dc, file_error *os.File
	count_record          [3] int64
	count_file            int64
)

func main() {
	{
		f, err := os.Create("torrent.sql")

		if err != nil {
			log.Fatal("Aborting torrent", err)
		}
		file_torrent = f
	}
	{
		f, err := os.Create("dc.sql")

		if err != nil {
			log.Fatal("Aborting DC++: ", err)
		}
		file_dc = f
	}
	{
		f, err := os.Create("error.sql")

		if err != nil {
			log.Fatal("Aborting DC++: ", err)
		}
		file_error = f
	}

	

	path := "."
	var last error
	if err := processPath(path); err != nil {
		log.Printf("error procesing %q: %v", path, err)
		last = err
	}

	fmt.Fprintf(file_dc, "commit;\n")
	fmt.Fprintf(file_torrent, "commit;\n")
	fmt.Fprintf(file_error, "commit;\n")
	
	defer file_dc.Close()
	defer file_torrent.Close()
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
			torrent := strings.HasPrefix(name, "torrent-")
			is_error_sql := strings.HasPrefix(name, "error-")
			if !strings.HasSuffix(fname, ".tar.gz") {
				continue
			}
			if err := processArchive(fname, torrent, is_error_sql); err != nil {
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

func processArchive(path string, is_torr bool, is_error_sql bool) error {
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
		if err := processFile(arch, is_torr, is_error_sql, h.Name); err != nil {
			return err
		}
	}
	return nil
}

func processFile(r io.Reader, is_torr bool, is_error_sql bool, n string) error {
	s := strings.Split(n, "-") // TODO opt
	date := s[3] + "." + s[2] + "." + s[1] + " " + s[4] + ":" + s[5] + ":" + s[6]
	dec := json.NewDecoder(r)
	ip := "" //TODO GeoIP
	if is_torr {
		return processTorrent(dec, ip, date)
	}
	if is_error_sql {
		return processError(dec, ip, date)
	}
	return processDC(dec, ip, date)
}

type Stats struct {
	Name string `json:"name"`
	Size int64  `json:"size,string"`
}

type DCStats struct {
	Stats
	TTH string `json:"tth"`
}

type TorrentStats struct {
	Stats
	SHA1 string `json:"sha1_torrent"`
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

func processTorrent(dec *json.Decoder, ip string, date string) error {
	var stats struct {
		List []TorrentStats `json:"array"`
	}
	if err := dec.Decode(&stats); err != nil {
		return err
	}
	for _, s := range stats.List {
		if err := importStat(s, ip, date); err != nil {
			return err
		}
	}
	return nil
}

func processDC(dec *json.Decoder, ip string, date string) error {
	var stats struct {
		List []DCStats `json:"array"`
	}
	if err := dec.Decode(&stats); err != nil {
		return err
	}
	for _, s := range stats.List {
		if err := importStat(s, ip, date); err != nil {
			return err
		}
	}
	return nil
}
func importStat(o interface{}, ip string, date string) error {
	switch o := o.(type) {
	case DCStats:
		count_record[0]++
		fmt.Fprintf(file_dc, "insert into t_tth values('%s','%s',%s',%d,'%s');\n", date, ip, o.Name, o.Size, o.TTH)
		if count_record[0]%1000 == 0 {
			fmt.Fprintf(file_dc, "commit;\n")
		}
	case TorrentStats:
		count_record[1]++
		fmt.Fprintf(file_torrent, "insert into t_sha1 values('%s','%s','%s',%d,'%s');\n", date, ip, o.Name, o.Size, o.SHA1)
		if count_record[0]%1000 == 0 {
			fmt.Fprintf(file_torrent, "commit;\n")
		}
	case ErrorStats:
		if strings.Contains(o.Error,"Media") {
		count_record[2]++
		fmt.Fprintf(file_error, "insert into t_error values('%s','%s','%s','%s','%s','%s');\n", date, ip, o.CID, o.Client, o.Current, o.Error)
		if count_record[0]%1000 == 0 {
			fmt.Fprintf(file_error, "commit;\n")
		}
       }
	default:
		log.Fatal("Error type: %+v\n", o)
	}
	return nil
}
