package aws

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

func toInt(s string, defVal int) int {
	i, err := strconv.Atoi(s)
	if err != nil {
		return defVal
	}
	return i
}

func NewClient() *Client {
	return &Client{
		Key:        os.Getenv("AWS_ACCESS_KEY_ID"),
		Secret:     os.Getenv("AWS_SECRET_ACCESS_KEY"),
		MaxRetries: toInt(os.Getenv("AWS_MAX_RETRIES"), 5),
	}
}

type Client struct {
	Key    string
	Secret string

	// If greater than zero, requests that fail will be retried 
	// up to this number of times
	MaxRetries int
}

func (c *Client) NewRequest(host, version string) *Request {
	return &Request{
		Host:    host,
		Version: version,
		Client:  *c,
		Params:  Params{},
	}
}

type Param struct {
	Key string
	Val string
}

func (p *Param) Encode() string {
	return p.Key + "=" + escape(p.Val)
}

type Params []*Param

func (p *Params) Add(key, val string) {
	*p = append(*p, &Param{key, val})
}

func (p *Params) Len() int {
	return len(*p)
}

func (p *Params) Less(i, j int) bool {
	a := *p
	return a[i].Key < a[j].Key
}

func (p *Params) Swap(i, j int) {
	a := *p
	a[i], a[j] = a[j], a[i]
}

func (p *Params) Encode() (s string) {
	parts := make([]string, len(*p))
	for i, param := range *p {
		parts[i] = param.Encode()
	}
	return strings.Join(parts, "&")
}

type Request struct {
	Host    string
	Version string

	Client
	Params

	encoded bool
}

func (r *Request) Encode() string {
	if !r.encoded {
		r.Add("AWSAccessKeyId", r.Key)
		r.Add("SignatureMethod", "HmacSHA256")
		r.Add("SignatureVersion", "2")
		r.Add("Version", r.Version)
		r.Add("Timestamp", time.Now().UTC().Format(time.RFC3339))

		sort.Sort(r)

		data := strings.Join([]string{
			"POST",
			r.Host,
			"/",
			r.Params.Encode(),
		}, "\n")

		h := hmac.New(sha256.New, []byte(r.Secret))
		h.Write([]byte(data))

		sig := base64.StdEncoding.EncodeToString(h.Sum([]byte{}))

		r.Add("Signature", sig)
		r.encoded = true
	}

	return r.Params.Encode()
}

type Header struct {
	RequestId string
}

type Error struct {
	Header
	Errors []struct {
		Code    string
		Message string
	} `xml:"Errors>Error"`
}

// Example:
//  aws: ->
//    AuthFailure: "There is a problem with your secret"
//    OMG: "You're servers are all gone!"
func (err *Error) Error() string {
	var s string
	for _, e := range err.Errors {
		s += fmt.Sprintf("\t%s: %q\n", e.Code, e.Message)
	}

	return fmt.Sprintf("aws: ->\n%s", s)
}

func Do(r *Request, v interface{}) error {
	var err error
	var res *http.Response
	for i := 0; i <= r.MaxRetries; i++ {
		if i > 0 {
			// sleep on retry
			jitter := rand.Int63n(200)
			ms := int64(math.Min(2000, 100*math.Pow(2, float64(i))))
			//fmt.Println("Retry: ", i, (ms+jitter))
			time.Sleep(time.Duration((ms + jitter) * int64(time.Millisecond)))
		}

		// charset=utf-8 is required by the SDB endpoint
		// otherwise it fails signature checking.
		// ec2 endpoint seems to be fine with it either way
		//start := time.Now()
		res, err = http.Post("https://"+r.Host,
			"application/x-www-form-urlencoded; charset=utf-8",
			bytes.NewBufferString(r.Encode()))
		//elap := time.Now().Sub(start)
		//fmt.Println(elap.Nanoseconds() / 1e6)

		if err == nil && res.StatusCode < 500 {
			// return immediately if no network error occurs and no 5xx HTTP status returned
			return unmarshal(res, v)
		}

	}

	if err == nil {
		return unmarshal(res, v)
	}
	return err
}

func unmarshal(res *http.Response, v interface{}) error {
	if res.StatusCode != http.StatusOK {
		e := new(Error)
		b, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return err
		}

		err = xml.Unmarshal(b, e)
		if err != nil {
			return err
		}
		return e
	}

	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}
	return xml.Unmarshal(b, v)
}

// Utils

// Used for debugging
type logReader struct {
	r io.Reader
}

func (lr *logReader) Read(b []byte) (n int, err error) {
	n, err = lr.r.Read(b)
	fmt.Print(string(b))
	return
}

////////////////////////////////
// url escaping -- taken from:
// https://github.com/robert-wallis/go-awssign/blob/master/aws.go

// modified from net.url because shouldEscape is
// overriden with an encodeQueryComponent 'if'
// http://golang.org/src/pkg/net/url/url.go?s=4017:4682#L175
func escape(s string) string {
	spaceCount, hexCount := 0, 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if shouldEscape(c) {
			hexCount++
		}
	}

	if spaceCount == 0 && hexCount == 0 {
		return s
	}

	t := make([]byte, len(s)+2*hexCount)
	j := 0
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case shouldEscape(c):
			t[j] = '%'
			t[j+1] = "0123456789ABCDEF"[c>>4]
			t[j+2] = "0123456789ABCDEF"[c&15]
			j += 3
		default:
			t[j] = s[i]
			j++
		}
	}
	return string(t)
}

// truncated from pkg net/url
// according to RFC 3986
func shouldEscape(c byte) bool {
	switch {
	// §2.3 Unreserved characters (alphanum)
	case 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' || '0' <= c && c <= '9':
		return false
	// §2.3 Unreserved characters (mark)
	case '-' == c, '_' == c, '.' == c, '~' == c:
		return false
	}
	return true
}
