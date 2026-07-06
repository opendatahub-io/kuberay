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
	// Ray start parameters
	RayHeadNumCpus       string
	RayHeadNumGpus       string
	RayWorkerNumCpus     string
	RayWorkerNumGpus     string
	RayEntrypointNumCpus string
	RayEntrypointNumGpus string
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
			// Ray start parameters - only update num-cpus to 8
			RayHeadNumCpus:       `"8"`,
			RayHeadNumGpus:       `""`, // Empty = no change
			RayWorkerNumCpus:     `""`, // Empty = no change
			RayWorkerNumGpus:     `""`, // Empty = no change
			RayEntrypointNumCpus: `""`, // Empty = no change
			RayEntrypointNumGpus: `""`, // Empty = no change
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
			// Ray start parameters - no changes for PR scenario
			RayHeadNumCpus:       `""`, // Empty = no change
			RayHeadNumGpus:       `""`, // Empty = no change
			RayWorkerNumCpus:     `""`, // Empty = no change
			RayWorkerNumGpus:     `""`, // Empty = no change
			RayEntrypointNumCpus: `""`, // Empty = no change
			RayEntrypointNumGpus: `""`, // Empty = no change
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

	// Print confirmation of what was updated in test files
	if strings.Contains(filePath, "test/") && strings.Contains(filePath, ".go") {
		printTestFileConfirmation(filePath)
	}
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

	// Also process Ray start parameters and entrypoint configurations
	ast.Inspect(file, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			processRayStartParams(call, spec)
			processEntrypointParams(call, spec)
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

	// Only process HeadPodTemplate and WorkerPodTemplate functions (case-insensitive)
	lowerFunctionName := strings.ToLower(functionName)
	if !strings.Contains(lowerFunctionName, "headpodtemplate") && !strings.Contains(lowerFunctionName, "workerpodtemplate") {
		return
	}

	isHeadFunction := strings.Contains(lowerFunctionName, "headpodtemplate")

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
				// Skip if newValue is empty (no change desired) or if values already match
				if newValue == "" || newValue == `""` || lit.Value == newValue {
					return
				}
				fmt.Printf("  Updating %s: %s -> %s (Function: %s, CPU: %t, Requests: %t)\n",
					lit.Value, lit.Value, newValue,
					map[bool]string{true: "Head", false: "Worker"}[isHeadFunction],
					isCPU, isRequests)
				lit.Value = newValue
			}
		}
	}
}

func getTargetValue(isHeadFunction, isCPU, isRequests bool, spec ResourceSpec) string {
	var value string
	switch {
	case isHeadFunction && isCPU && isRequests:
		value = spec.HeadCPURequest
	case isHeadFunction && !isCPU && isRequests:
		value = spec.HeadMemoryRequest
	case isHeadFunction && isCPU && !isRequests:
		value = spec.HeadCPULimit
	case isHeadFunction && !isCPU && !isRequests:
		value = spec.HeadMemoryLimit
	case !isHeadFunction && isCPU && isRequests:
		value = spec.WorkerCPURequest
	case !isHeadFunction && !isCPU && isRequests:
		value = spec.WorkerMemoryRequest
	case !isHeadFunction && isCPU && !isRequests:
		value = spec.WorkerCPULimit
	case !isHeadFunction && !isCPU && !isRequests:
		value = spec.WorkerMemoryLimit
	}
	// Return empty string if value is empty (no change desired)
	if value == `""` {
		return ""
	}
	return value
}

// printTestFileConfirmation shows what was updated in test files
func printTestFileConfirmation(filePath string) {
	fmt.Printf("\n=== Test File Update Confirmation: %s ===\n", filePath)

	// Check for Ray start parameters
	if strings.Contains(filePath, "rayjob_lightweight_test.go") {
		fmt.Printf("Checking Ray start parameters in test file...\n")
		content, err := os.ReadFile(filePath)
		if err != nil {
			fmt.Printf("Could not read file for confirmation: %v\n", err)
			return
		}

		lines := strings.Split(string(content), "\n")
		for i, line := range lines {
			if strings.Contains(line, "num-cpus") {
				fmt.Printf("Line %d: %s\n", i+1, strings.TrimSpace(line))
			}
			if strings.Contains(line, "num-gpus") {
				fmt.Printf("Line %d: %s\n", i+1, strings.TrimSpace(line))
			}
			if strings.Contains(line, "WithEntrypointNumCpus") {
				fmt.Printf("Line %d: %s\n", i+1, strings.TrimSpace(line))
			}
			if strings.Contains(line, "WithEntrypointNumGpus") {
				fmt.Printf("Line %d: %s\n", i+1, strings.TrimSpace(line))
			}
		}
	}

	// Check for pod resource limits
	if strings.Contains(filePath, "support.go") {
		fmt.Printf("Checking pod resource limits in support file...\n")
		content, err := os.ReadFile(filePath)
		if err != nil {
			fmt.Printf("Could not read file for confirmation: %v\n", err)
			return
		}

		lines := strings.Split(string(content), "\n")
		inHeadFunction := false
		inWorkerFunction := false

		for i, line := range lines {
			lowerLine := strings.ToLower(line)
			if strings.Contains(lowerLine, "func headpodtemplateapplyconfiguration") {
				inHeadFunction = true
				inWorkerFunction = false
			} else if strings.Contains(lowerLine, "func workerpodtemplateapplyconfiguration") {
				inHeadFunction = false
				inWorkerFunction = true
			} else if strings.Contains(line, "func ") && !strings.Contains(lowerLine, "headpodtemplate") && !strings.Contains(lowerLine, "workerpodtemplate") {
				inHeadFunction = false
				inWorkerFunction = false
			}

			if (inHeadFunction || inWorkerFunction) && strings.Contains(line, "resource.MustParse") {
				funcType := "Head"
				if inWorkerFunction {
					funcType = "Worker"
				}
				fmt.Printf("Line %d (%s): %s\n", i+1, funcType, strings.TrimSpace(line))
			}
		}
	}

	fmt.Printf("=== End Confirmation ===\n\n")
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

// processRayStartParams processes WithRayStartParams calls
func processRayStartParams(call *ast.CallExpr, spec ResourceSpec) {
	if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
		if sel.Sel.Name == "WithRayStartParams" && len(call.Args) > 0 {
			if comp, ok := call.Args[0].(*ast.CompositeLit); ok {
				// Only process if this appears to be head group params (contains dashboard-host)
				if isHeadGroupRayStartParams(comp) {
					processRayStartParamsMap(comp, spec)
				}
			}
		}
	}
}

// isHeadGroupRayStartParams checks if this looks like head group params
func isHeadGroupRayStartParams(comp *ast.CompositeLit) bool {
	for _, elt := range comp.Elts {
		if kv, ok := elt.(*ast.KeyValueExpr); ok {
			if keyLit, ok := kv.Key.(*ast.BasicLit); ok && keyLit.Kind == token.STRING {
				if keyLit.Value == `"dashboard-host"` {
					return true
				}
			}
		}
	}
	return false
}

// processRayStartParamsMap processes the map[string]string in WithRayStartParams
func processRayStartParamsMap(comp *ast.CompositeLit, spec ResourceSpec) {
	for _, elt := range comp.Elts {
		if kv, ok := elt.(*ast.KeyValueExpr); ok {
			if keyLit, ok := kv.Key.(*ast.BasicLit); ok && keyLit.Kind == token.STRING {
				if valueLit, ok := kv.Value.(*ast.BasicLit); ok && valueLit.Kind == token.STRING {
					updateRayStartParam(keyLit.Value, valueLit, spec)
				}
			}
		}
	}
}

// updateRayStartParam updates a specific Ray start parameter
func updateRayStartParam(key string, valueLit *ast.BasicLit, spec ResourceSpec) {
	var newValue string
	switch key {
	case `"num-cpus"`:
		newValue = spec.RayHeadNumCpus
	case `"num-gpus"`:
		newValue = spec.RayHeadNumGpus
	}

	// Skip if newValue is empty (no change desired)
	if newValue == "" || newValue == `""` {
		return
	}

	// Skip if values already match
	if valueLit.Value == newValue {
		return
	}

	fmt.Printf("  Updating Ray start param %s: %s -> %s\n", key, valueLit.Value, newValue)
	valueLit.Value = newValue
}

// processEntrypointParams processes WithEntrypointNumCpus and WithEntrypointNumGpus calls
func processEntrypointParams(call *ast.CallExpr, spec ResourceSpec) {
	if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
		switch sel.Sel.Name {
		case "WithEntrypointNumCpus":
			if len(call.Args) > 0 {
				updateEntrypointParam(call.Args[0], spec.RayEntrypointNumCpus, "EntrypointNumCpus")
			}
		case "WithEntrypointNumGpus":
			if len(call.Args) > 0 {
				updateEntrypointParam(call.Args[0], spec.RayEntrypointNumGpus, "EntrypointNumGpus")
			}
		}
	}
}

// updateEntrypointParam updates entrypoint parameter values
func updateEntrypointParam(arg ast.Expr, newValue, paramName string) {
	// Skip if newValue is empty (no change desired)
	if newValue == "" || newValue == `""` {
		return
	}

	// Remove quotes from newValue for numeric comparison
	numericNewValue := strings.Trim(newValue, `"`)

	if lit, ok := arg.(*ast.BasicLit); ok && lit.Kind == token.INT {
		// Skip if values already match
		if lit.Value == numericNewValue {
			return
		}
		fmt.Printf("  Updating %s: %s -> %s\n", paramName, lit.Value, numericNewValue)
		lit.Value = numericNewValue
	}
}
