package main

/*
This is a minimal sample application, demonstrating how to set up an RSS feed for regular polling of new channels/items.
Build & run with:
 $ 6g example.go && 6l example.6 && ./6.out

---

status
1 = first entered
2 = mp3 downloaded
3 = submitted to API
4 = API finished, json ready

*/

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"fmt"
	gcfg "code.google.com/p/gcfg"
	"io"
	"net/http"
	"net/url"
	"os"
//	"path"
	rss "github.com/jteeuwen/go-pkg-rss"
	"strings"
	"time"
)

type Config struct {
	Main struct {
		Mysql   string
		Rss     string
		Timeout int
	}
}

type Item struct {
	id        int
	mts       time.Time
	sts       time.Time
	status    string
	title     string
	pubDate   time.Time
	guid      string
	url       string
	length    int
	typer     string
	json      string
}

var db      *sql.DB
var err     error
var version string
var cfg     Config

func main() {
	err = gcfg.ReadFileInto(&cfg, "feed.ini")
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: Failed to parse gcfg data: %s\n", err.Error())
	} else {
		err = OpenDatabase(cfg.Main.Mysql); if err != nil { os.Exit(1) }
		defer db.Close()

		// This sets up a new feed and polls it for new channels/items.
		// Invoke it with 'go PollFeed(...)' to have the polling performed in a separate goroutine, so you can continue with the rest of your program.
		PollFeed(cfg.Main.Rss, cfg.Main.Timeout)
	}
}

func OpenDatabase(pipefnam string) error {
	fmt.Printf("INFO: MYSQL: connecting to: %s\n", pipefnam);
	db, err = sql.Open("mysql", pipefnam);
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: MYSQL: can't connect to database[1]: %s\n", err.Error())
		return err
	}
	err = db.Ping()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: MYSQL: can't connect to database[2]: %s\n", err.Error())
		return err
	}
	db.QueryRow("SELECT VERSION()").Scan(&version)
	fmt.Println("INFO: Connected to MySQL server version ", version)
	return nil
}

func PollFeed(uri string, timeout int) {
	feed := rss.New(timeout, true, chanHandler, itemHandler)

//	for { // do only once...

		fmt.Printf("INFO: fetching %s\n", uri);
		if err = feed.Fetch(uri, nil); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: %s: %s\n", uri, err)
			return
		}

//		<-time.After(time.Duration(feed.SecondsTillUpdate() * 1e9))
//	}

}

func chanHandler(feed *rss.Feed, newchannels []*rss.Channel) {
	fmt.Printf("%d new channel(s) in %s\n", len(newchannels), feed.Url)
	for i := 0; i < len(newchannels); i++ {
		fmt.Printf("%d. %s\n", i + 1, newchannels[i].Description);
	}
}

// do: http://api.yactraq.com/stream-status?url= encode(newitems[i].Englosures[0].Url) &email=&transcript=true&start=1&href=http%3A%2F%2Fwww.yactraq.com%2Fspeech2topics.html&adset=12m1oeebz9tv00t&tx=1
// eg: http://api.yactraq.com/stream-status?url=http%3A%2F%2Fcontent.dar.fm%2Fstorage%2Flockerplay%2F07d8e75c15de4399c1505850fe584a08.mp3%3Fsid%3D6faf37ef273fbb55dc5928e9af71dc60%26partner_token%3D5994303431&email=&transcript=true&start=1&href=http%3A%2F%2Fwww.yactraq.com%2Fspeech2topics.html&adset=12m1oeebz9tv00t&tx=1
// but we need to monitor this.
func itemHandler(feed *rss.Feed, ch *rss.Channel, newitems []*rss.Item) {
	fmt.Printf("%d new item(s) in %s\n", len(newitems), feed.Url)
	for i := 0; i < len(newitems); i++ {
		fmt.Printf("%d. %s @ %s\n", i + 1, newitems[i].Description, newitems[i].Enclosures[0].Url);
		handleit(newitems[i].Enclosures[0].Url);
	}
}

func handleit(mp3address string) {
	var finished bool
	finished, err = handle_database   (mp3address); if finished { return }
	finished, err = handle_yactraq_api(mp3address)
}

func handle_yactraq_api(mp3address string) (bool, error) {
	var (
		Url        *url.URL
	//	resp       *http.Response
		Servlet     string        = "http://api.yactraq.com"
		Referer     string        = "http://www.yactraq.com/speech2topics.html"
		parameters  url.Values    = url.Values{}
	)

	Url, err = url.Parse(Servlet);
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: can't connect to: %s ... %v\n", Servlet, err);
		return false, err
	}
	Url.Path += "/stream-status"
	parameters.Add("url"       , mp3address       );
	parameters.Add("email"     , ""               );
	parameters.Add("transcript", "true"           );
	parameters.Add("start"     , "1"              );
	parameters.Add("href"      , Referer          );
	parameters.Add("adset"     , "12m1oeebz9tv00t"); // 12m1oeebz9tv00t | Dollario+ (60s) ** | Dollario+ 20130927 | { "backup_set":"combined-20131125", "topic_set":"dollario-20130927", "sphinx_set":"combined_20140215a-40K", "refresh": 60 }
	parameters.Add("tx"        , "1"              );
	Url.RawQuery = parameters.Encode()

	fmt.Printf("INFO: Encoded URL is %q\n", Url.String())
/*	resp, err = http.Get(Url.String())
	defer resp.Body.Close()
	if err != nil {
		fmt.Printf("-a-\n");
		fmt.Fprintf(os.Stderr, "ERROR: can't read '%s' ... %v\n", Url.String(), err);
		return false, err
	}
	fmt.Printf("-b-\n");
	fmt.Printf("INFO: Response is '%s'\n", resp)
	fmt.Printf("INFO: status = %s\n", resp.Status);
	fmt.Printf("INFO: statuscode = %d\n", resp.StatusCode);
	fmt.Printf("INFO: protocol = %s\n", resp.Proto);
	fmt.Printf("INFO: length = %d\n", resp.ContentLength);
*/
	return true, nil // not sure whether true... needs to be determined first!
}

func handle_database(mp3address string) (bool, error) {
	var (
		var_item   Item
		var_url    *url.URL
		var_fnam   string
		var_path   []string
		out        *os.File
		resp       *http.Response
	//	contents   []byte
		n          int64
	)
	var_item, err = query_item(mp3address)
	if &var_item != nil {
		switch {
		  case var_item.status == "+": fmt.Printf("INFO: %s already done...\n", mp3address); return true, nil;
		  case var_item.status == " ": fmt.Printf("INFO: %s initiating...\n"  , mp3address);
		}
	} else {
		fmt.Printf("INFO: %s adding...\n", mp3address);
	}
	var_url, err = url.Parse(mp3address)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: can't parse '%s' ... %v\n", mp3address, err)
		return false, err
	}

	var_path = strings.Split(var_url.Path, "/")
	var_fnam = var_path[len(var_path) - 1]

	fmt.Printf("INFO: opening %q\n", var_fnam)
	out, err = os.Create(var_fnam)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: can't create '%s' ... %v\n", var_fnam, err);
		return false, err;
	}
	defer out.Close()

	fmt.Printf("INFO: getting %q\n", mp3address)
	resp, err = http.Get(mp3address)
	defer resp.Body.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: can't get '%s' ... %v\n", mp3address, err);
		return false, err
	}

//	contents, err = ioutil.ReadAll(response.Body)
//	if err != nil {
//		fmt.Fprintf(os.Stderr, "ERROR: can't read '%s' ... %v\n", mp3address, err);
//		return false, err
//	}
//	fmt.Printf("%s\n", string(contents))

	n, err = io.Copy(out, resp.Body)
	fmt.Printf("%d bytes written to %s\n", n, var_fnam);

	return false, nil;
}

func query_item(mp3address string) (Item, error) {
	var item Item
	err = db.QueryRow("SELECT * FROM items WHERE url = ?", mp3address).Scan(&item.id, &item.mts, &item.sts, &item.status, &item.title, &item.pubDate, &item.guid, &item.url, &item.length, &item.typer, &item.json);
	switch {
	  case err == sql.ErrNoRows: return Item{}, nil; // not sure what...
	  case err != nil          : fmt.Fprintf(os.Stderr, "ERROR: can't read row: %v\n", err); return Item{}, err;
	}
	return item, nil
}

