package extract

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/ledongthuc/pdf"
)

// PDF extraction is a native text-layer reader, not OCR or a layout/table
// replacement for pdfplumber. Input/pages/decoded streams/operations/output are
// bounded; unsupported or malformed files produce explicit diagnostics.
const pdfStreamLimit = 2 * 1024 * 1024

var errPDFLimit = errors.New("pdf_resource_limit")
var errPDFTextCap = errors.New("pdf_text_cap")

type pdfInput struct {
	reader   *bytes.Reader
	read     int
	deadline time.Time
}

func (r *pdfInput) ReadAt(p []byte, offset int64) (int, error) {
	if r.read > 128*1024*1024 || time.Now().After(r.deadline) {
		panic(errPDFLimit)
	}
	n, err := r.reader.ReadAt(p, offset)
	r.read += n
	return n, err
}
func checkPDFStream(v pdf.Value, limit int64) {
	switch v.Kind() {
	case pdf.Null:
		return
	case pdf.Array:
		if v.Len() > 128 {
			panic(errPDFLimit)
		}
		for i := 0; i < v.Len(); i++ {
			checkPDFStream(v.Index(i), limit)
		}
	case pdf.Stream:
		r := v.Reader()
		defer r.Close()
		n, err := io.Copy(io.Discard, io.LimitReader(r, limit+1))
		if err != nil {
			panic(err)
		}
		if n > limit {
			panic(errPDFLimit)
		}
	}
}
func ExtractPDF(body []byte) (title, text, errorCode string) {
	if len(body) > MaxInputBytes {
		return "", "", "pdf_too_large"
	}
	if !bytes.HasPrefix(body, []byte("%PDF-")) {
		return "", "", "pdf_invalid_header"
	}
	var out strings.Builder
	defer func() {
		if recovered := recover(); recovered != nil {
			if recovered == errPDFTextCap {
				text = trim(out.String())
				errorCode = ""
				return
			}
			title = ""
			text = ""
			if recovered == errPDFLimit {
				errorCode = "pdf_resource_limit"
			} else {
				errorCode = "pdf_parse_error"
			}
		}
	}()
	input := &pdfInput{reader: bytes.NewReader(body), deadline: time.Now().Add(5 * time.Second)}
	reader, err := pdf.NewReader(input, int64(len(body)))
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "encrypt") {
			return "", "", "pdf_encrypted"
		}
		return "", "", "pdf_parse_error"
	}
	title = Cut(reader.Trailer().Key("Info").Key("Title").Text(), 300)
	pages := min(reader.NumPage(), 80)
	if pages < 0 {
		return title, "", "pdf_parse_error"
	}
	count, operations := 0, 0
	appendText := func(value string) {
		value = Cut(value, MaxText-count)
		out.WriteString(value)
		count += chars(value)
		if count >= MaxText {
			panic(errPDFTextCap)
		}
	}
	for number := 1; number <= pages; number++ {
		if time.Now().After(input.deadline) {
			panic(errPDFLimit)
		}
		page := reader.Page(number)
		if page.V.IsNull() {
			continue
		}
		stream := page.V.Key("Contents")
		checkPDFStream(stream, pdfStreamLimit)
		names := page.Fonts()
		if len(names) > 128 {
			panic(errPDFLimit)
		}
		fonts := map[string]pdf.TextEncoding{}
		for _, name := range names {
			font := page.Font(name)
			checkPDFStream(font.V.Key("ToUnicode"), pdfStreamLimit/2)
			fonts[name] = font.Encoder()
		}
		var encoding pdf.TextEncoding
		emit := func(raw string) {
			if encoding == nil {
				appendText(raw)
				return
			}
			// Bound each font expansion before appending it to the text budget.
			for len(raw) > 0 {
				take := min(len(raw), 16)
				appendText(encoding.Decode(raw[:take]))
				raw = raw[take:]
			}
		}
		if number > 1 {
			appendText("\n\n")
		}
		pdf.Interpret(stream, func(stack *pdf.Stack, op string) {
			operations++
			if operations > 200000 || time.Now().After(input.deadline) {
				panic(errPDFLimit)
			}
			if stack.Len() > 4096 {
				panic(errPDFLimit)
			}
			args := make([]pdf.Value, stack.Len())
			for i := len(args) - 1; i >= 0; i-- {
				args[i] = stack.Pop()
			}
			switch op {
			case "BT", "T*":
				appendText("\n")
			case "Tf":
				if len(args) >= 1 {
					encoding = fonts[args[0].Name()]
				}
			case "Tj":
				if len(args) >= 1 {
					emit(args[len(args)-1].RawString())
				}
			case "'", "\"":
				if len(args) >= 1 {
					appendText("\n")
					emit(args[len(args)-1].RawString())
				}
			case "TJ":
				if len(args) > 0 {
					array := args[0]
					if array.Len() > 100000 {
						panic(errPDFLimit)
					}
					for i := 0; i < array.Len(); i++ {
						if v := array.Index(i); v.Kind() == pdf.String {
							emit(v.RawString())
						}
					}
				}
			}
		})
	}
	text = trim(out.String())
	if text == "" {
		return title, "", "pdf_no_text_layer"
	}
	if !strings.ContainsAny(text, "\x00") {
		return title, text, ""
	}
	return title, strings.ReplaceAll(text, "\x00", ""), ""
}
