package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"strings"
)

// ResourceSpec defines the target values for different resource types
type ResourceSpec struct {
	HeadCPURequest      string
	HeadMemoryRequest   string
	HeadCPULimit        string
	HeadMemoryLimit     string
	WorkerCPURequest    string
	WorkerMemoryRequest string
	WorkerCPULimit      string
	WorkerMemoryLimit   string
}

// getResourceSpec returns the appropriate resource specification based on the scenario
func getResourceSpec(scenario string) (ResourceSpec, error) {
	switch scenario {
	case "build-image":
		return ResourceSpec{
			HeadCPURequest:      `"500m"`,
			HeadMemoryRequest:   `"6G"`,
			HeadCPULimit:        `"2000m"`,
			HeadMemoryLimit:     `"10G"`,
			WorkerCPURequest:    `"500m"`,
			WorkerMemoryRequest: `"1G"`, // Stays the same
			WorkerCPULimit:      `"1000m"`,
			WorkerMemoryLimit:   `"3G"`,
		}, nil
	case "pr":
		return ResourceSpec{
			HeadCPURequest:      `"500m"`,
			HeadMemoryRequest:   `"1G"`,
			HeadCPULimit:        `"2000m"`,
			HeadMemoryLimit:     `"3G"`,
			WorkerCPURequest:    `"300m"`,
			WorkerMemoryRequest: `"1G"`, // Stays the same
			WorkerCPULimit:      `"1000m"`,
			WorkerMemoryLimit:   `"3G"`,
		}, nil
	default:
		return ResourceSpec{}, fmt.Errorf("unknown scenario: %s. Available scenarios: build-image, pr", scenario)
	}
}

func main() {
	var scenario = flag.String("scenario", "build-image", "Resource scenario to apply (build-image, pr)")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		fmt.Println("Usage: go run update-resources.go [-scenario build-image|pr] <path-to-support.go>")
		fmt.Println("  -scenario: Resource scenario to apply")
		fmt.Println("    build-image: Build image scenario with higher resources")
		fmt.Println("    pr:          PR scenario with lower resources for head memory")
		os.Exit(1)
	}

	filePath := args[0]

	// Get the appropriate resource specification based on scenario
	spec, err := getResourceSpec(*scenario)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Applying scenario '%s' to %s\n", *scenario, filePath)
	err = updateResourcesInFile(filePath, spec)
	if err != nil {
		fmt.Printf("Error updating resources: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Successfully updated resources in %s\n", filePath)
}

func updateResourcesInFile(filePath string, spec ResourceSpec) error {
	// Parse the Go file
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filePath, nil, parser.ParseComments)
	if err != nil {
		return fmt.Errorf("failed to parse file: %w", err)
	}

	// Walk the AST and modify resource values
	ast.Inspect(file, func(n ast.Node) bool {
		if funcDecl, ok := n.(*ast.FuncDecl); ok {
			if funcDecl.Name != nil {
				processFunctionDecl(funcDecl, spec)
			}
		}
		return true
	})

	// Write the modified AST back to the file
	outputFile, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outputFile.Close()

	err = format.Node(outputFile, fset, file)
	if err != nil {
		return fmt.Errorf("failed to format and write file: %w", err)
	}

	return nil
}

func processFunctionDecl(funcDecl *ast.FuncDecl, spec ResourceSpec) {
	functionName := funcDecl.Name.Name

	// Only process HeadPodTemplate and WorkerPodTemplate functions
	if !strings.Contains(functionName, "HeadPodTemplate") && !strings.Contains(functionName, "WorkerPodTemplate") {
		return
	}

	isHeadFunction := strings.Contains(functionName, "HeadPodTemplate")

	// Track the order of resource.MustParse calls within WithRequests and WithLimits
	ast.Inspect(funcDecl, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			processResourceCall(call, isHeadFunction, spec)
		}
		return true
	})
}

func processResourceCall(call *ast.CallExpr, isHeadFunction bool, spec ResourceSpec) {
	// Look for WithRequests or WithLimits calls
	if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
		isRequests := sel.Sel.Name == "WithRequests"
		isLimits := sel.Sel.Name == "WithLimits"

		if isRequests || isLimits {
			processResourceBlock(call, isHeadFunction, isRequests, spec)
		}
	}
}

func processResourceBlock(call *ast.CallExpr, isHeadFunction, isRequests bool, spec ResourceSpec) {
	// Look for the ResourceList composite literal
	if len(call.Args) > 0 {
		if comp, ok := call.Args[0].(*ast.CompositeLit); ok {
			processResourceList(comp, isHeadFunction, isRequests, spec)
		}
	}
}

func processResourceList(comp *ast.CompositeLit, isHeadFunction, isRequests bool, spec ResourceSpec) {
	for _, elt := range comp.Elts {
		if kv, ok := elt.(*ast.KeyValueExpr); ok {
			processResourceEntry(kv, isHeadFunction, isRequests, spec)
		}
	}
}

func processResourceEntry(kv *ast.KeyValueExpr, isHeadFunction, isRequests bool, spec ResourceSpec) {
	// Determine if this is CPU or Memory based on the key
	var isCPU bool
	if sel, ok := kv.Key.(*ast.SelectorExpr); ok {
		if ident, ok := sel.X.(*ast.Ident); ok && ident.Name == "corev1" {
			isCPU = sel.Sel.Name == "ResourceCPU"
		}
	}

	// Find the resource.MustParse call in the value
	if call, ok := kv.Value.(*ast.CallExpr); ok {
		if isResourceMustParseCall(call) && len(call.Args) > 0 {
			if lit, ok := call.Args[0].(*ast.BasicLit); ok {
				newValue := getTargetValue(isHeadFunction, isCPU, isRequests, spec)
				if newValue != "" && lit.Value != newValue {
					fmt.Printf("  Updating %s: %s -> %s (Function: %s, CPU: %t, Requests: %t)\n",
						lit.Value, lit.Value, newValue,
						map[bool]string{true: "Head", false: "Worker"}[isHeadFunction],
						isCPU, isRequests)
					lit.Value = newValue
				}
			}
		}
	}
}

func getTargetValue(isHeadFunction, isCPU, isRequests bool, spec ResourceSpec) string {
	switch {
	case isHeadFunction && isCPU && isRequests:
		return spec.HeadCPURequest
	case isHeadFunction && !isCPU && isRequests:
		return spec.HeadMemoryRequest
	case isHeadFunction && isCPU && !isRequests:
		return spec.HeadCPULimit
	case isHeadFunction && !isCPU && !isRequests:
		return spec.HeadMemoryLimit
	case !isHeadFunction && isCPU && isRequests:
		return spec.WorkerCPURequest
	case !isHeadFunction && !isCPU && isRequests:
		return spec.WorkerMemoryRequest
	case !isHeadFunction && isCPU && !isRequests:
		return spec.WorkerCPULimit
	case !isHeadFunction && !isCPU && !isRequests:
		return spec.WorkerMemoryLimit
	}
	return ""
}

// isResourceMustParseCall checks if the call expression is resource.MustParse
func isResourceMustParseCall(call *ast.CallExpr) bool {
	if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
		if ident, ok := sel.X.(*ast.Ident); ok {
			return ident.Name == "resource" && sel.Sel.Name == "MustParse"
		}
	}
	return false
}
