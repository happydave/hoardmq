// Command licensecheck applies the dependency licence policy to the module
// graph and exits non-zero when any module fails it. It writes nothing.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/happydave/hoardmq/internal/modlicense"
)

func main() {
	mods, err := modlicense.List(context.Background(), ".")
	if err != nil {
		fmt.Fprintln(os.Stderr, "licensecheck:", err)
		os.Exit(2)
	}
	reports := modlicense.Classify(mods)
	for _, r := range reports {
		fmt.Printf("%-50s %-10s %s\n", r.Module.Path, r.Module.Version, r.Family)
	}
	violations := modlicense.Violations(reports)
	if len(violations) == 0 {
		fmt.Printf("licensecheck: %d third-party modules, all within policy\n", len(reports))
		return
	}
	for _, r := range violations {
		reason := "licence family " + string(r.Family) + " is not allowed"
		if r.File == "" {
			reason = "no licence file found"
		}
		fmt.Fprintf(os.Stderr, "licensecheck: %s %s: %s\n", r.Module.Path, r.Module.Version, reason)
	}
	os.Exit(1)
}
