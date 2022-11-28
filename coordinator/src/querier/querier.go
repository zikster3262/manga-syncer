package querier

import (
	"log"
	"net/http"

	"github.com/zikster3262/shared-lib/page"
	"github.com/zikster3262/shared-lib/source"

	"github.com/PuerkitoBio/goquery"
)

func ScapeMangaPage(mp source.SourceSQL) (m []page.Page) {
	// Request the HTML page.
	res, err := http.Get(mp.Manga_URL)
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
	doc.Find(mp.Home_Pattern).Each(func(i int, s *goquery.Selection) {

		v, _ := s.Attr("href")
		t, _ := s.Attr("title")

		mn := page.Page{
			Url:   v,
			Title: t,
		}

		if mp.Append {
			mn.Url = mp.Manga_URL + v
		}

		m = append(m, mn)

	})

	return m
}

func AsuraScanMangaPage(url string) (chapters []string) {
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
	doc.Find("ul.clstyle li div.chbox div.eph-num a").Each(func(i int, s *goquery.Selection) {

		v, _ := s.Attr("href")

		// fmt.Printf("%v\n", v)
		chapters = append(chapters, v)

	})
	return chapters
}

func ReadMng(url string, appendURL bool) []string {

	// ReadMng("https://www.readmng.com", false))
	// Request the HTML page.
	var chapters []string
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
	doc.Find("div.popularToday div.galeriContent.listUpdates div.miniListCard div.miniListDesc h2 a").Each(func(i int, s *goquery.Selection) {
		v, _ := s.Attr("href")
		if appendURL {
			chapters = append(chapters, url+v)
		} else {
			chapters = append(chapters, v)
		}

	})
	return chapters
}
