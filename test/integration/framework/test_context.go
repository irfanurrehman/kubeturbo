package framework

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"k8s.io/klog"
)

// MultiStringFlag is a flag for passing multiple parameters using same flag
type MultiStringFlag []string

// String returns string representation of the node groups.
func (flag *MultiStringFlag) String() string {
	return "[" + strings.Join(*flag, " ") + "]"
}

// Set adds a new configuration.
func (flag *MultiStringFlag) Set(value string) error {
	*flag = strings.Split(value, ",")
	return nil
}

type TestContextType struct {
	KubeConfig        string
	KubeContext       string
	TestNamespace     string
	SingleCallTimeout time.Duration

	// NodeGroups is useful for the node provision and suspend via cloud provider tests
	NodeGroups *MultiStringFlag
}

var TestContext *TestContextType = &TestContextType{}

func registerFlags(t *TestContextType) {
	flag.StringVar(&t.KubeConfig, "k8s-kubeconfig", os.Getenv("KUBECONFIG"),
		"Path to kubeconfig containing embedded authinfo.")
	flag.StringVar(&t.KubeContext, "k8s-context", "",
		"kubeconfig context to use/override. If unset, will use value from 'current-context'.")
	flag.StringVar(&t.TestNamespace, "test-namespace", DefaultTestNS,
		fmt.Sprintf("The namespace that will be used as the seed name for tests.  If unset, will default to %q.", DefaultTestNS))
	flag.DurationVar(&t.SingleCallTimeout, "single-call-timeout", DefaultSingleCallTimeout,
		fmt.Sprintf("The maximum duration of a single call.  If unset, will default to %v", DefaultSingleCallTimeout))
	t.NodeGroups = new(MultiStringFlag)
	flag.Var(t.NodeGroups, "cp-node-groups", "The node group names when initialising the cloud provider with scale "+
		"min and max values. e.g. --cp-node-groups=1:10:nodegroup1,1:10:nodegroup2")
}

func validateFlags(t *TestContextType) {
	if len(t.KubeConfig) == 0 {
		klog.Fatalf("kubeconfig is required")
	}
}

func ParseFlags() {
	registerFlags(TestContext)
	flag.Parse()
	validateFlags(TestContext)
}
