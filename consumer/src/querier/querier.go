package querier

import (
	"log"
	"net/http"

	"github.com/PuerkitoBio/goquery"
	"github.com/zikster3262/shared-lib/page"
)

func ScapeMangaPage(url, pattern, title string, id int64, appendUrl bool) (m []page.Page) {
	// Request the HTML page.
	res, err := http.Get(url)
	if err != nil {
		log.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		log.Fatalf("status code error: %d %s", res.StatusCode, res.Status)
	}

	// Load the HTML document
	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		log.Fatal(err)
	}

	// Find the review items
	doc.Find(pattern).Each(func(i int, s *goquery.Selection) {

		href, _ := s.Attr("href")

		mn := page.Page{
			Url:       href,
			Title:     title,
			Source_Id: id,
		}

		if appendUrl {
			mn.Url = url + href
		}

		m = append(m, mn)

	})

	return m
}
