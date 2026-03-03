package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

var (
	// By default, this example reads from a public dataset containing the text of
	// King Lear. Set this option to choose a different input file or glob.
	input = flag.String("input", "gs://apache-beam-samples/shakespeare/kinglear.txt", "File(s) to read.")

	// Set this required option to specify where to write the output.
	output = flag.String("output", "", "Output file (required).")
)

func init() {
	// register.DoFnXxY registers a struct DoFn so that it can be correctly
	// serialized and does some optimization to avoid runtime reflection. Since
	// extractFn has 3 inputs and 0 outputs, we use register.DoFn3x0 and provide
	// its input types as its constraints (if it had any outputs, we would add
	// those as constraints as well). Struct DoFns must be registered for a
	// pipeline to run.
	register.DoFn3x0[context.Context, string, func(string)](&extractFn{})
	// register.FunctionXxY registers a functional DoFn to optimize execution at
	// runtime. formatFn has 2 inputs and 1 output, so we use
	// register.Function2x1.
	register.Function2x1(formatFn)
	// register.EmitterX is optional and will provide some optimization to make
	// things run faster. Any emitters (functions that produce output for the next
	// step) should be registered. Here we register all emitters with the
	// signature func(string).
	register.Emitter1[string]()
}

var (
	wordRE          = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)
	empty           = beam.NewCounter("extract", "emptyLines")
	smallWordLength = flag.Int("small_word_length", 9, "length of small words (default: 9)")
	smallWords      = beam.NewCounter("extract", "smallWords")
	lineLen         = beam.NewDistribution("extract", "lineLenDistro")
)

type extractFn struct {
	SmallWordLength int `json:"smallWordLength"`
}

func (f *extractFn) ProcessElement(ctx context.Context, line string, emit func(string)) {
	lineLen.Update(ctx, int64(len(line)))
	if len(strings.TrimSpace(line)) == 0 {
		empty.Inc(ctx, 1)
	}
	for _, word := range wordRE.FindAllString(line, -1) {
		// increment the counter for small words if length of words is
		// less than small_word_length
		if len(word) < f.SmallWordLength {
			smallWords.Inc(ctx, 1)
		}
		emit(word)
	}
}

// formatFn is a functional DoFn that formats a word and its count as a string.
func formatFn(w string, c int) string {
	return fmt.Sprintf("%s: %v", w, c)
}

func CountWords(s beam.Scope, lines beam.PCollection) beam.PCollection {
	s = s.Scope("CountWords")

	// Convert lines of text into individual words.
	col := beam.ParDo(s, &extractFn{SmallWordLength: *smallWordLength}, lines)

	// Count the number of times each word occurs.
	return stats.Count(s, col)
}

func main() {
	flag.Parse()
	// beam.Init() is an initialization hook that must be called on startup. On
	// distributed runners, it is used to intercept control.
	beam.Init()

	// Input validation is done as usual. Note that it must be after Init().
	if *output == "" {
		log.Fatal("No output provided")
	}

	p := beam.NewPipeline()
	s := p.Root()

	lines := textio.Read(s, *input)
	counted := CountWords(s, lines)
	formatted := beam.ParDo(s, formatFn, counted)
	textio.Write(s, *output, formatted)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatal("Failed to execute job: %v", err)
	}
}
