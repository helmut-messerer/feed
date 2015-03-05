package main

/*
This is a minimal sample application, demonstrating how to set up an RSS feed for regular polling of new channels/items.
Build & run with:
 $ 6g example.go && 6l example.6 && ./6.out

---

status
  = not yet
. = not yet
+ = done

*/

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"fmt"
	gabs "github.com/jeffail/gabs"
	gcfg "code.google.com/p/gcfg"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
//	"path"
	"path/filepath"
	rss "github.com/jteeuwen/go-pkg-rss"
//	"strconv"
//	"strings"
	"time"
)

type Config struct {
	Main struct {
		Mysql   string
		Rss     string
		Timeout int
		Path    string
		Save    bool
		Servlet string
		Referer string
		Status  string
		Adset   string
	}
}

type Item struct {
	Id        int64
	Mts       time.Time
	Sts       time.Time
	Status    int64
	Title     string
	PubDate   time.Time
	Guid      string
	Url       string
	Filename  string
	Length    int64
	Type      string
	Json      string
	Todo      bool
}

type Entry struct {
	Address     string
	Title       string
	Description string
	PubDate     string
	Guid        string
	Length      int64
	Type        string
}

type Status struct {
	Id          int64
	Name        string
	Todo        bool
}

// FeedError is an error implementation that includes a time and message.
type FeedError struct {
	When time.Time
	What string
}

var db       *sql.DB
var err       error
var version   string
var cfg       Config
var statuses  map[int64]Status
var statusrv  map[string]int64

func(e FeedError) Error   () string { return fmt.Sprintf("%v: %v", e.When, e.What) }
func(e Entry    ) ToString() string { return fmt.Sprintf("[Entry - Address: %v, Title: %v, Description: %v, PubDate: %v Guid: %v, Length: %v, Type: %v]", e.Address, e.Title, e.Description, e.PubDate, e.Guid, e.Length, e.Type); }
func NewEntry(newitem *rss.Item) *Entry { return &Entry { Address: newitem.Enclosures[0].Url, Title: newitem.Title, Description: newitem.Description, PubDate: newitem.PubDate, Guid: *newitem.Guid, Length: newitem.Enclosures[0].Length, Type: newitem.Enclosures[0].Type } }
func NewStatus(id int64, name string, todo bool) Status { return Status { Id: id, Name: name, Todo: todo }; }

func main() {
	statuses  = make(map[int64]Status)
	statusrv  = make(map[string]int64)
	err = gcfg.ReadFileInto(&cfg, "feed.ini")
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: Failed to parse gcfg data: %s\n", err.Error())
	} else {
		err = OpenDatabase(cfg.Main.Mysql); if err != nil { os.Exit(1) }; defer db.Close()
		err = ReadStatuses();               if err != nil { os.Exit(1) };

		// This sets up a new feed and polls it for new channels/items.
		// Invoke it with 'go PollFeed(...)' to have the polling performed in a separate goroutine, so you can continue with the rest of your program.
		PollFeed(cfg.Main.Rss, cfg.Main.Timeout)
	}
}


func OpenDatabase(pipefnam string) error {
	fmt.Printf("INFO: DB: connecting to: %s\n", pipefnam);
	db, err = sql.Open("mysql", pipefnam);
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: DB: can't connect [1]: %s\n", err.Error())
		return err
	}
	err = db.Ping()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: DB: can't connect [2]: %s\n", err.Error())
		return err
	}
	db.QueryRow("SELECT VERSION()").Scan(&version)
	fmt.Println("INFO: Connected to DB server version ", version)
	return nil
}

func ReadStatuses() error {
	var (
		rows        *sql.Rows
		id           int64
		status_str   string
		todo         bool
		cnt          int64
	)
	fmt.Printf("INFO: reading statuses\n");
	rows, err = db.Query("SELECT id,name,todo FROM statuses ORDER BY id");
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: can't query statuses: %v\n", err.Error());
		return err;
	}

	cnt      = 1;
	for rows.Next() {
		err = rows.Scan(&id, &status_str, &todo);
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: can't scan row #%d: %v\n", cnt, err.Error());
			return err;
		}
		fmt.Printf("INFO: id=%d, status=%s, todo=%v\n", id, status_str, todo);
		statuses[id        ] = NewStatus(id, status_str, todo);
		statusrv[status_str] = id;
		cnt++;
	}
	err = rows.Close();
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: can't close statuses: %v\n", err.Error());
		return err;
	}
	fmt.Printf("INFO: %d statuses read\n", cnt);
	return nil;
}

func PollFeed(uri string, timeout int) {
	feed := rss.New(timeout, true, chanHandler, itemHandler)

//	for { // do only once...

		fmt.Printf("INFO: fetching %s\n", uri);
		if err = feed.Fetch(uri, nil); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: can't fetch %s: %s\n", uri, err)
			return
		}

//		<-time.After(time.Duration(feed.SecondsTillUpdate() * 1e9))
//	}

}

func chanHandler(feed *rss.Feed, newchannels []*rss.Channel) {
	fmt.Printf("%d new channel(s) in %s\n", len(newchannels), feed.Url)
	for i := 0; i < len(newchannels); i++ {
		fmt.Printf("Channel %d. %s\n", i + 1, newchannels[i].Description);
	}
}

/* do: http://api.yactraq.com/stream-status?url= encode(newitems[i].Englosures[0].Url) &email=&transcript=true&start=1&href=http%3A%2F%2Fwww.yactraq.com%2Fspeech2topics.html&adset=12m1oeebz9tv00t&tx=1
   eg: http://api.yactraq.com/stream-status?url=http%3A%2F%2Fcontent.dar.fm%2Fstorage%2Flockerplay%2F07d8e75c15de4399c1505850fe584a08.mp3%3Fsid%3D6faf37ef273fbb55dc5928e9af71dc60%26partner_token%3D5994303431&email=&transcript=true&start=1&href=http%3A%2F%2Fwww.yactraq.com%2Fspeech2topics.html&adset=12m1oeebz9tv00t&tx=1
   but we need to monitor this.

   <item>
     <title>KCBS_4 (KCBS) 02/12/15 01</title>
     <link>http://www.dar.fm</link>
     <description>KCBS_4 (KCBS) 02/12/15 01</description>
     <pubDate>Thu, 12 Feb 2015 16:19:31 +0000</pubDate>
     <guid isPermaLink="false">7c69a3cd6f79894e7f4bd5d808477d8e</guid>
     <enclosure url="http://content.dar.fm/storage/lockerplay/7c69a3cd6f79894e7f4bd5d808477d8e.mp3?sid=6da7a10d32f0d1306a160aceaa9ca99d&amp;partner_token=5994303431" length="7172284" type="audio/mpeg"/>
   </item>
*/

func itemHandler(feed *rss.Feed, ch *rss.Channel, newitems []*rss.Item) {
	fmt.Printf("%d new item(s) in %s\n", len(newitems), feed.Url)
	for i := 0; i < len(newitems); i++ {
		fmt.Printf("Item %d. %s @ %s\n", i + 1, newitems[i].Description, newitems[i].Enclosures[0].Url);
		handleit(NewEntry(newitems[i]));
	}
}

func handleit(entry *Entry) {
	fmt.Printf("INFO: handle %s\n", entry.ToString());
	var finished bool
	finished, err = handle_database   (entry); if finished { return }
	finished, err = handle_yactraq_api(entry); if finished { return }
}

func handle_yactraq_api(entry *Entry) (bool, error) {
	var (
		apiaddress   string
		contents   []byte
		jsonValue    string
		status_int   int64
		status       Status
		statusOk     bool
	)
	apiaddress, err      = get_api_url(entry);                                 if err != nil { return false, err; };
	contents  , err      = read_api_json(apiaddress, entry.Address);           if err != nil { return false, err; }; jsonValue = string(contents); // fmt.Printf("%s\n", jsonValue);
	status_int, err      = update_status(contents);                            if err != nil { return false, err; };
	            err      = update_json(entry.Address, jsonValue, status_int);  if err != nil { return false, err; };
	status    , statusOk = statuses[status_int];
	if !statusOk {
		fmt.Fprintf(os.Stderr, "ERROR: can't find status %d", status_int);
		return false, FeedError { When: time.Now(), What: fmt.Sprintf("can't find status %d", status_int) };
	}
	return !status.Todo, nil;
}

func get_api_url(entry *Entry) (string, error) {
	var (
		Url         *url.URL
		Servlet      string        = cfg.Main.Servlet
		Referer      string        = cfg.Main.Referer
		parameters   url.Values    = url.Values{}
	)
	Url, err = url.Parse(Servlet);
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: can't connect to: %s ... %v\n", Servlet, err);
		return "", err
	}
	Url.Path += cfg.Main.Status
	parameters.Add("url"       , entry.Address );
	parameters.Add("email"     , ""            );
	parameters.Add("transcript", "true"        );
	parameters.Add("start"     , "1"           );
	parameters.Add("href"      , Referer       );
	parameters.Add("adset"     , cfg.Main.Adset); // 12m1oeebz9tv00t | Dollario+ (60s) ** | Dollario+ 20130927 | { "backup_set":"combined-20131125", "topic_set":"dollario-20130927", "sphinx_set":"combined_20140215a-40K", "refresh": 60 }
	parameters.Add("tx"        , "1"           );
	Url.RawQuery = parameters.Encode()
	fmt.Printf("INFO: Encoded URL is %q\n", Url.String())
	return Url.String(), nil;
}

func update_status(contents []byte) (int64, error) {
	var (
		jsonParsed  *gabs.Container
		jsonStatus   string
		jsonOk       bool
		statusOk     bool
		status_int   int64
	)
	jsonParsed, err = gabs.ParseJSON(contents)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: can't parse json: %v\n", err.Error());
		return -1, err;
	}
	jsonStatus, jsonOk = jsonParsed.Path("status").Data().(string)
	if !jsonOk {
		fmt.Fprintf(os.Stderr, "ERROR: can't get json status!\n");
		return -1, FeedError { When: time.Now(), What: "can't get json status" };
	}
	fmt.Printf("INFO: status = '%s', ok = '%v'\n", jsonStatus, jsonOk);
	status_int, statusOk = statusrv[jsonStatus];
	if !statusOk {
		status_int, err = insert_status(jsonStatus, true); if err != nil { return -1, err }
	}
	return status_int, nil;
}

func insert_status(status_str string, todo bool) (int64, error) {
	var (
		st         *sql.Stmt
		result      sql.Result
		status_int  int64
	)
	st, err = db.Prepare("INSERT INTO statuses (name,todo) VALUES(?,?)");
	if err != nil {
		fmt.Fprint(os.Stderr, "ERROR: can't prepare insert_status statement: %v\n", err);
		return -1, err;
	}
	result, err = st.Exec(status_str, todo);
	if err != nil {
		fmt.Fprint(os.Stderr, "ERROR: can't execute insert_status statement: %v\n", err);
		return -1, err
	}
	status_int, err = result.LastInsertId();
	if err != nil {
		fmt.Fprint(os.Stderr, "ERROR: can't get insert_status id: %v\n", err);
		return -1, err
	}
	statuses[status_int] = NewStatus(status_int, status_str, todo);
	statusrv[status_str] = status_int;
	st.Close();
	return status_int, nil;
}

func read_api_json(apiaddress string, mp3address string) ([]byte, error) {
	var (
		resp        *http.Response
		contents   []byte
	)
	resp, err = http.Get(apiaddress);
	defer resp.Body.Close()
	if err != nil {
		fmt.Printf("-a-\n");
		fmt.Fprintf(os.Stderr, "ERROR: can't read '%s' ... %v\n", apiaddress, err);
		return nil, err
	}
	fmt.Printf("INFO: Response is '%s'\n", resp              );
	fmt.Printf("INFO: status = %s\n"     , resp.Status       );
	fmt.Printf("INFO: statuscode = %d\n" , resp.StatusCode   );
	fmt.Printf("INFO: protocol = %s\n"   , resp.Proto        );
	fmt.Printf("INFO: length = %d\n"     , resp.ContentLength);

	contents, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: can't read '%s' ... %v\n", mp3address, err);
		return nil, err
	}
	return contents, nil
}

func update_json(mp3address string, json string, status_int int64) error {
	var (
		st         *sql.Stmt
		result      sql.Result
		raff        int64
	)
	st, err = db.Prepare("UPDATE items SET json=?,status=? WHERE url=?")
	if err != nil {
		fmt.Fprint(os.Stderr, "ERROR: can't prepare update_json statement: %v\n", err);
		return err;
	}
	result, err = st.Exec(mp3address, status_int, json);
	if err != nil {
		fmt.Fprint(os.Stderr, "ERROR: can't execute update_json statement: %v\n", err);
		return err
	}
	raff, err = result.RowsAffected();
	if err != nil {
		fmt.Fprint(os.Stderr, "ERROR: can't get affected rows for update_json statement: %v\n", err);
		return err
	}
	if raff <= 0 {
		fmt.Print("INFO: both json and status(%d) for '%s' haven't changed\n", status_int, mp3address);
	}
	return nil;
}

func handle_database(entry *Entry) (bool, error) {
	var (
		var_item    Item
	)
	var_item, err = query_item(entry.Address)
	if var_item != (Item{}) {
		fmt.Printf("INFO: %s found, todo = %v\n", entry.Address, var_item.Todo);
		return !var_item.Todo, nil;
	//	switch {
	//	  case var_item.Status == "+": fmt.Printf("INFO: %s already done...\n"                       , entry.Address); return true , nil;
	//	  case var_item.Status == ".": fmt.Printf("INFO: %s already in DB but not yet processed...\n", entry.Address); return false, nil;
	//	  case var_item.Status == " ": fmt.Printf("INFO: %s initiating...\n"                         , entry.Address);
	//	}
	} else {
		err = store_new_item(&var_item, entry);
		if err != nil { return false, err; }
	}
	err = save_mp3(&var_item, entry);
	if err != nil { return false, err }
	return false, nil;
}

func store_new_item(var_item *Item, entry *Entry) error {
	var (
		st         *sql.Stmt
		result      sql.Result
	)
	fmt.Printf("INFO: %s adding...\n", entry.Address);
	var_item.Status     = 2;
	var_item.Title      = entry.Title;
	var_item.PubDate, _ = time.Parse(time.RFC1123Z, entry.PubDate);
	var_item.Guid       = entry.Guid;
	var_item.Url        = entry.Address;
	var_item.Filename   = entry.Guid + ".mp3";
	var_item.Length     = entry.Length;
	var_item.Type       = entry.Type;
	var_item.Json       = "";

	st, err = db.Prepare("INSERT INTO items (status,title,pubDate,guid,url,filename,length,type,json) VALUES (?,?,?,?,?,?,?,?,?)")
	if err != nil {
		fmt.Fprint(os.Stderr, "ERROR: can't prepare store_new_item statement: %v\n", err);
		return err;
	}
	result, err = st.Exec(var_item.Status, var_item.Title, var_item.PubDate, var_item.Guid, var_item.Url, var_item.Filename, var_item.Length, var_item.Type, var_item.Json);
	if err != nil {
		fmt.Fprint(os.Stderr, "ERROR: can't execute store_new_item statement: %v\n", err);
		return err;
	}
	var_item.Id, err = result.LastInsertId();
	if err != nil {
		fmt.Fprint(os.Stderr, "ERROR: can't get store_new_item id: %v\n", err);
		return err;
	}
	st.Close();
	return nil;
}

func save_mp3(var_item *Item, entry *Entry) error {
	var (
		var_fnam    string
		fi          os.FileInfo
	)
	if(!cfg.Main.Save) { return nil; }
	var_fnam = filepath.Clean(cfg.Main.Path + string(os.PathSeparator) + var_item.Filename)
	fi, err = os.Stat(var_fnam)
	if (err != nil) && !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "ERROR: can't stat '%s' ... %v\n", var_fnam, err)
		return err
	} else if os.IsNotExist(err) || (fi.Size() <= 0) {
		err = download_mp3(var_fnam, entry.Address)
		if err != nil { return err }
	} else {
		fmt.Printf("INFO: mp3 already downloaded: %q, size %d\n", var_fnam, fi.Size());
	}
	return nil;
}

func download_mp3(var_fnam string, mp3address string) error {
	var (
		out        *os.File
		resp       *http.Response
		n          int64
	)
	fmt.Printf("INFO: creating %q\n", var_fnam)
	out, err = os.Create(var_fnam)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: can't create '%s' ... %v\n", var_fnam, err);
		return err;
	}
	defer out.Close()

	fmt.Printf("INFO: getting %q\n", mp3address)
	resp, err = http.Get(mp3address)
	defer resp.Body.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: can't get '%s' ... %v\n", mp3address, err);
		return err
	}

	n, err = io.Copy(out, resp.Body)
	fmt.Printf("INFO: %d bytes written to %s\n", n, var_fnam); // check whether length matches?

	return nil;
}

func query_item(mp3address string) (Item, error) {
	var item Item
	err = db.QueryRow("SELECT i.* FROM items i LEFT JOIN statuses s ON (i.status=s.id) WHERE i.url=?", mp3address).Scan(&item.Id, &item.Mts, &item.Sts, &item.Status, &item.Title, &item.PubDate, &item.Guid, &item.Url, &item.Filename, &item.Length, &item.Type, &item.Json);
	switch {
	  case err == sql.ErrNoRows: return Item{}, nil; // not sure what...
	  case err != nil          : fmt.Fprintf(os.Stderr, "ERROR: can't read row: %v\n", err); return Item{}, err;
	}
	return item, nil
}

