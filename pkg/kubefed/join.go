package kubefed

import (
	"fmt"
	"k8s.io/client-go/dynamic"
	kubeclient "k8s.io/client-go/kubernetes"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"sigs.k8s.io/kubefed/pkg/kubefedctl"
	"sigs.k8s.io/kubefed/pkg/kubefedctl/util"

	"sigs.k8s.io/kubefed/pkg/kubefedctl/options"
)

func JoinKubefed(kubefedkubeconfigPath, kubefedNamespace, joiningClusterName string, kubeturboClusterConfig *rest.Config) (*kubeclient.Clientset, dynamic.Interface, error) {
	fedConfig := util.NewFedConfig(clientcmd.NewDefaultPathOptions())
	fedClientConfig := fedConfig.GetClientConfig("", kubefedkubeconfigPath)

	kubefedClusterName, err := currentContext(fedClientConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to get current context from kubefed config: %v", err)
	}

	kubefedClusterConfig, err := fedClientConfig.ClientConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to get kubefed cluster client config: %v", err)
	}

	scope, err := options.GetScopeFromKubeFedConfig(kubefedClusterConfig, kubefedNamespace)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to get target kubefed scope (namespaced/cluster-scoped): %v", err)
	}

	kubefedClient, err := kubeclient.NewForConfig(kubefedClusterConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to get kubefed client: %v", err)
	}

	kubefedDynClient, err := dynamic.NewForConfig(kubefedClusterConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to get kubefed dynamic client: %v", err)
	}

	return kubefedClient, kubefedDynClient, kubefedctl.JoinCluster(kubefedClusterConfig, kubeturboClusterConfig, kubefedNamespace,
		kubefedClusterName, joiningClusterName, "", scope, false, false)
}

// CurrentContext retrieves the current context from the provided config.
func currentContext(config clientcmd.ClientConfig) (string, error) {
	rawConfig, err := config.RawConfig()
	if err != nil {
		return "", err
	}
	return rawConfig.CurrentContext, nil
}
