package main

import (
	"net/http"
	"bufio"
	"sync"
	"strings"
	"fmt"
	"strconv"
	"sort"
	"time"
	"log"
	"encoding/json"
)

const (
	posSpeaker   = 0
	posTopic     = 1
	posDate      = 2
	posWordCount = 3
)

type lineResult struct {
	line string
	url  string
	err  error
}

type analysisResult struct {
	MostSpeeches string
	MostSecurity string
	LeastWordy   string
	Errors       []string
}

func main() {
	http.HandleFunc("/evaluation", handleEvaluation)
	log.Println("Server startup done.")
	log.Fatal(http.ListenAndServe("localhost:8000", nil))
}

func handleEvaluation(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	urls, ok := r.URL.Query()["url"]

	if !ok || len(urls) < 1 {
		log.Println("Url Param 'url' is missing")
		return
	}
	log.Printf("/evaluation called with urls: %s \n", urls)

	var wg sync.WaitGroup
	var readLines = make(chan lineResult)
	var analysisResult = make(chan analysisResult)
	for _, url := range urls {
		wg.Add(1)
		go readURLLines(&wg, readLines, url)
	}

	go analyseLines(readLines, analysisResult)

	wg.Wait()
	close(readLines)
	finalResult := <-analysisResult

	log.Printf("%+v\n", finalResult)
	log.Printf("%+v\n", finalResult.Errors)
	log.Printf("Time: %s\n", time.Since(start))

	jsonResult, err := json.Marshal(finalResult)
	if err != nil {
		log.Fatalf("JSON marshaling failed: %s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(jsonResult)
}

func readURLLines(wg *sync.WaitGroup, lines chan<- lineResult, url string) {
	defer wg.Done()
	resp, err := http.Get(url)
	if err != nil {
		lines <- lineResult{"", url, err}
		return
	}
	defer resp.Body.Close()
	scanner := bufio.NewScanner(resp.Body)
	scanner.Scan() // ignore first line
	for scanner.Scan() {
		if err := scanner.Err(); err != nil {
			lines <- lineResult{"", url, err}
			return
		}
		lines <- lineResult{scanner.Text(), url, nil}
	}
}

func analyseLines(lines <-chan lineResult, result chan<- analysisResult) {
	var res analysisResult
	var speeches2013 = make(map[string]int)
	var speechesSecurity = make(map[string]int)
	var speechesWords = make(map[string]int)
	for line := range lines {
		if line.err != nil {
			res.Errors = append(res.Errors, line.err.Error())
			continue
		}
		substrings := strings.Split(line.line, ",")
		if len(substrings) != 4 {
			res.Errors = append(res.Errors, fmt.Sprintf("Not enough elements. URL: '%s' Line: '%s'", line.url, line.line))
			continue
		}
		wordCount, err := strconv.ParseInt(strings.TrimSpace(substrings[posWordCount]), 10, 0)
		if err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("Word count invalid format. URL: '%s' Line: '%s'", line.url, line.line))
			continue
		}
		speaker := substrings[posSpeaker]
		speechesWords[speaker] += int(wordCount)
		if strings.Contains(substrings[posDate], "2013") {
			speeches2013[speaker]++
		}
		if strings.TrimSpace(substrings[posTopic]) == "Innere Sicherheit" {
			speechesSecurity[speaker]++
		}
	}
	res.MostSpeeches = sortMapGetResult(&speeches2013, true)
	res.MostSecurity = sortMapGetResult(&speechesSecurity, true)
	res.LeastWordy = sortMapGetResult(&speechesWords, false)

	result <- res
}

func sortMapGetResult(mp *map[string]int, getFirst bool) string {
	if len(*mp) == 0 {
		return "null"
	}
	pl := make(PairList, 0)
	for k, v := range *mp {
		pl = append(pl, Pair{k, v})
	}
	if getFirst {
		sort.Sort(pl)
	} else {
		sort.Sort(sort.Reverse(pl))
	}
	if len(pl) > 1 && pl[0].Value == pl[1].Value {
		return "null"
	}
	return pl[0].Key
}

type Pair struct {
	Key   string
	Value int
}

type PairList []Pair

func (p PairList) Len() int           { return len(p) }
func (p PairList) Less(i, j int) bool { return p[i].Value > p[j].Value }
func (p PairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
