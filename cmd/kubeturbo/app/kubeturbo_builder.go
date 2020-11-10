package app

import (
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	agg "github.com/turbonomic/kubeturbo/pkg/discovery/worker/aggregation"
	"github.com/turbonomic/kubeturbo/pkg/resourcemapping"

	clusterclient "github.com/openshift/cluster-api/pkg/client/clientset_generated/clientset"
	apiv1 "k8s.io/api/core/v1"
	apiextclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/typed/apiextensions/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	versionhelper "k8s.io/apimachinery/pkg/version"
	"k8s.io/apiserver/pkg/server/healthz"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"

	kubeturbo "github.com/turbonomic/kubeturbo/pkg"
	"github.com/turbonomic/kubeturbo/pkg/util"
	"github.com/turbonomic/kubeturbo/test/flag"

	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/pflag"
	"github.com/turbonomic/kubeturbo/pkg/kubeclient"
)

const (
	// The default port for vmt service server
	KubeturboPort                     = 10265
	DefaultKubeletPort                = 10255
	DefaultKubeletHttps               = false
	defaultVMPriority                 = -1
	defaultVMIsBase                   = true
	defaultDiscoveryIntervalSec       = 600
	DefaultValidationWorkers          = 10
	DefaultValidationTimeout          = 60
	DefaultDiscoveryWorkers           = 4
	DefaultDiscoveryTimeoutSec        = 180
	DefaultDiscoverySamples           = 10
	DefaultDiscoverySampleIntervalSec = 60
)

var (
	defaultSccSupport = []string{"restricted"}

	// these variables will be deprecated. Keep it here for backward compatibility only
	k8sVersion        = "1.8"
	noneSchedulerName = "turbo-no-scheduler"
)

type cleanUp func()

// VMTServer has all the context and params needed to run a Scheduler
// TODO: leaderElection is disabled now because of dependency problems.
type VMTServer struct {
	Port                 int
	Address              string
	Master               string
	K8sTAPSpec           string
	TestingFlagPath      string
	KubeConfig           string
	BindPodsQPS          float32
	BindPodsBurst        int
	DiscoveryIntervalSec int

	// LeaderElection componentconfig.LeaderElectionConfiguration

	EnableProfiling bool

	// To stitch the Nodes in Kubernetes cluster with the VM from the underlying cloud or
	// hypervisor infrastructure: either use VM UUID or VM IP.
	// If the underlying infrastructure is VMWare, AWS instances, or Azure instances, VM's UUID is used.
	UseUUID bool

	// VMPriority: priority of VM in supplyChain definition from kubeturbo, should be less than 0;
	VMPriority int32
	// VMIsBase: Is VM is the base template from kubeturbo, when stitching with other VM probes, should be false;
	VMIsBase bool

	// Kubelet related config
	KubeletPort          int
	EnableKubeletHttps   bool
	UseNodeProxyEndpoint bool

	// The cluster processor related config
	ValidationWorkers int
	ValidationTimeout int

	// Discovery related config
	DiscoveryWorkers    int
	DiscoveryTimeoutSec int

	// Data sampling discovery related config
	DiscoverySamples           int
	DiscoverySampleIntervalSec int

	// The Openshift SCC list allowed for action execution
	sccSupport []string

	// Force the use of self-signed certificates.
	// The default is true.
	ForceSelfSignedCerts bool

	// Don't try to move pods which have volumes attached
	// If set to false kubeturbo can still try to move such pods.
	FailVolumePodMoves bool

	// The Cluster API namespace
	ClusterAPINamespace string

	// Busybox image uri used for cpufreq getter job
	BusyboxImage string

	// Strategy to aggregate Container utilization data on ContainerSpec entity
	containerUtilizationDataAggStrategy string
	// Strategy to aggregate Container usage data on ContainerSpec entity
	containerUsageDataAggStrategy string
}

// NewVMTServer creates a new VMTServer with default parameters
func NewVMTServer() *VMTServer {
	s := VMTServer{
		Port:       KubeturboPort,
		Address:    "127.0.0.1",
		VMPriority: defaultVMPriority,
		VMIsBase:   defaultVMIsBase,
	}
	return &s
}

// AddFlags adds flags for a specific VMTServer to the specified FlagSet
func (s *VMTServer) AddFlags(fs *pflag.FlagSet) {
	fs.IntVar(&s.Port, "port", s.Port, "The port that kubeturbo's http service runs on.")
	fs.StringVar(&s.Address, "ip", s.Address, "the ip address that kubeturbo's http service runs on.")
	fs.StringVar(&s.Master, "master", s.Master, "The address of the Kubernetes API server (overrides any value in kubeconfig).")
	fs.StringVar(&s.K8sTAPSpec, "turboconfig", s.K8sTAPSpec, "Path to the config file.")
	fs.StringVar(&s.TestingFlagPath, "testingflag", s.TestingFlagPath, "Path to the testing flag.")
	fs.StringVar(&s.KubeConfig, "kubeconfig", s.KubeConfig, "Path to kubeconfig file with authorization and master location information.")
	fs.BoolVar(&s.EnableProfiling, "profiling", false, "Enable profiling via web interface host:port/debug/pprof/.")
	fs.BoolVar(&s.UseUUID, "stitch-uuid", true, "Use VirtualMachine's UUID to do stitching, otherwise IP is used.")
	fs.IntVar(&s.KubeletPort, "kubelet-port", DefaultKubeletPort, "The port of the kubelet runs on.")
	fs.BoolVar(&s.EnableKubeletHttps, "kubelet-https", DefaultKubeletHttps, "Indicate if Kubelet is running on https server.")
	fs.BoolVar(&s.UseNodeProxyEndpoint, "use-node-proxy-endpoint", false, "Indicate if Kubelet queries should be routed through APIServer node proxy endpoint.")
	fs.BoolVar(&s.ForceSelfSignedCerts, "kubelet-force-selfsigned-cert", true, "Indicate if we must use self-signed cert.")
	fs.BoolVar(&s.FailVolumePodMoves, "fail-volume-pod-moves", true, "Indicate if kubeturbo should fail to move pods which have volumes attached. Default is set to true.")
	fs.StringVar(&k8sVersion, "k8sVersion", k8sVersion, "[deprecated] the kubernetes server version; for openshift, it is the underlying Kubernetes' version.")
	fs.StringVar(&noneSchedulerName, "noneSchedulerName", noneSchedulerName, "[deprecated] a none-exist scheduler name, to prevent controller to create Running pods during move Action.")
	fs.IntVar(&s.DiscoveryIntervalSec, "discovery-interval-sec", defaultDiscoveryIntervalSec, "The discovery interval in seconds.")
	fs.IntVar(&s.ValidationWorkers, "validation-workers", DefaultValidationWorkers, "The validation workers")
	fs.IntVar(&s.ValidationTimeout, "validation-timeout-sec", DefaultValidationTimeout, "The validation timeout in seconds.")
	fs.IntVar(&s.DiscoveryWorkers, "discovery-workers", DefaultDiscoveryWorkers, "The number of discovery workers.")
	fs.IntVar(&s.DiscoveryTimeoutSec, "discovery-timeout-sec", DefaultDiscoveryTimeoutSec, "The discovery timeout in seconds for each discovery worker.")
	fs.IntVar(&s.DiscoverySamples, "discovery-samples", DefaultDiscoverySamples, "The number of resource usage data samples to be collected from kubelet in each full discovery cycle. This should be no larger than 60.")
	fs.IntVar(&s.DiscoverySampleIntervalSec, "discovery-sample-interval", DefaultDiscoverySampleIntervalSec, "The discovery interval in seconds to collect additional resource usage data samples from kubelet. This should be no smaller than 10 seconds.")
	fs.StringSliceVar(&s.sccSupport, "scc-support", defaultSccSupport, "The SCC list allowed for executing pod actions, e.g., --scc-support=restricted,anyuid or --scc-support=* to allow all.")
	fs.StringVar(&s.ClusterAPINamespace, "cluster-api-namespace", "default", "The Cluster API namespace.")
	fs.StringVar(&s.BusyboxImage, "busybox-image", "busybox", "The complete image uri used for fallback node cpu frequency getter job.")
	fs.StringVar(&s.containerUtilizationDataAggStrategy, "cnt-utilization-data-agg-strategy", agg.DefaultContainerUtilizationDataAggStrategy, "Container utilization data aggregation strategy.")
	fs.StringVar(&s.containerUsageDataAggStrategy, "cnt-usage-data-agg-strategy", agg.DefaultContainerUsageDataAggStrategy, "Container usage data aggregation strategy.")
}

// create an eventRecorder to send events to Kubernetes APIserver
func createRecorder(kubecli *kubernetes.Clientset) record.EventRecorder {
	// Create a new broadcaster which will send events we generate to the apiserver
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.Infof)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubecli.CoreV1().RESTClient()).Events(apiv1.NamespaceAll)})
	// this EventRecorder can be used to send events to this EventBroadcaster
	// with the given event source.
	return eventBroadcaster.NewRecorder(scheme.Scheme, apiv1.EventSource{Component: "kubeturbo"})
}

func (s *VMTServer) createKubeConfigOrDie() *restclient.Config {
	kubeConfig, err := clientcmd.BuildConfigFromFlags(s.Master, s.KubeConfig)
	if err != nil {
		glog.Errorf("Fatal error: failed to get kubeconfig:  %s", err)
		os.Exit(1)
	}
	// This specifies the number and the max number of query per second to the api server.
	kubeConfig.QPS = 20.0
	kubeConfig.Burst = 30

	return kubeConfig
}

func (s *VMTServer) createKubeClientOrDie(kubeConfig *restclient.Config) *kubernetes.Clientset {
	kubeClient, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		glog.Errorf("Fatal error: failed to create kubeClient:%v", err)
		os.Exit(1)
	}

	return kubeClient
}

func (s *VMTServer) CreateKubeletClientOrDie(kubeConfig *restclient.Config, fallbackClient *kubernetes.Clientset, busyboxImage string, useProxyEndpoint bool) *kubeclient.KubeletClient {
	kubeletClient, err := kubeclient.NewKubeletConfig(kubeConfig).
		WithPort(s.KubeletPort).
		EnableHttps(s.EnableKubeletHttps).
		ForceSelfSignedCerts(s.ForceSelfSignedCerts).
		// Timeout(to).
		Create(fallbackClient, busyboxImage, useProxyEndpoint)
	if err != nil {
		glog.Errorf("Fatal error: failed to create kubeletClient: %v", err)
		os.Exit(1)
	}

	return kubeletClient
}

func (s *VMTServer) checkFlag() error {
	if s.KubeConfig == "" && s.Master == "" {
		glog.Warningf("Neither --kubeconfig nor --master was specified.  Using default API client.  This might not work.")
	}

	if s.Master != "" {
		glog.V(3).Infof("Master is %s", s.Master)
	}

	if s.TestingFlagPath != "" {
		flag.SetPath(s.TestingFlagPath)
	}

	ip := net.ParseIP(s.Address)
	if ip == nil {
		return fmt.Errorf("wrong ip format:%s", s.Address)
	}

	if s.Port < 1 {
		return fmt.Errorf("Port[%d] should be bigger than 0.", s.Port)
	}

	if s.KubeletPort < 1 {
		return fmt.Errorf("[KubeletPort[%d] should be bigger than 0.", s.KubeletPort)
	}

	return nil
}

// Run runs the specified VMTServer.  This should never exit.
func (s *VMTServer) Run() {
	if err := s.checkFlag(); err != nil {
		glog.Fatalf("Check flag failed: %v. Abort.", err.Error())
	}

	kubeConfig := s.createKubeConfigOrDie()
	glog.V(3).Infof("kubeConfig: %+v", kubeConfig)

	kubeClient := s.createKubeClientOrDie(kubeConfig)

	dynamicClient, err := dynamic.NewForConfig(kubeConfig)
	if err != nil {
		glog.Fatalf("Failed to generate dynamic client for kubernetes target: %v", err)
	}

	apiExtClient, err := apiextclient.NewForConfig(kubeConfig)
	if err != nil {
		glog.Fatalf("Failed to generate apiExtensions client for kubernetes target: %v", err)
	}

	util.K8sAPIDeploymentGV, err = discoverk8sAPIResourceGV(kubeClient, util.DeploymentResName)
	if err != nil {
		glog.Warningf("Failure in discovering k8s deployment API group/version: %v", err.Error())
	}
	glog.V(2).Infof("Using group version %v for k8s deployments", util.K8sAPIDeploymentGV)

	util.K8sAPIReplicasetGV, err = discoverk8sAPIResourceGV(kubeClient, util.ReplicaSetResName)
	if err != nil {
		glog.Warningf("Failure in discovering k8s replicaset API group/version: %v", err.Error())
	}
	glog.V(2).Infof("Using group version %v for k8s replicasets", util.K8sAPIReplicasetGV)

	glog.V(3).Infof("Turbonomic config path is: %v", s.K8sTAPSpec)

	k8sTAPSpec, err := kubeturbo.ParseK8sTAPServiceSpec(s.K8sTAPSpec, kubeConfig.Host)
	if err != nil {
		glog.Fatalf("Failed to generate correct TAP config: %v", err.Error())
	}

	featureFlags := ""
	if k8sTAPSpec.FeatureGates != nil {
		for _, f := range k8sTAPSpec.FeatureGates.DisabledFeatures {
			featureFlag := fmt.Sprintf("%s=%s", f, "false")
			if featureFlags == "" {
				featureFlags = featureFlag
			} else {
				featureFlags = fmt.Sprintf("%s,%s", featureFlags, featureFlag)
			}
		}
	}
	err = utilfeature.DefaultFeatureGate.Set(featureFlags)
	if err != nil {
		glog.Fatalf("Invalid Feature Gates: %v", err)
	}

	// Collect target and probe info such as master host, server version, probe container image, etc
	k8sTAPSpec.CollectK8sTargetAndProbeInfo(kubeConfig, kubeClient)

	kubeletClient := s.CreateKubeletClientOrDie(kubeConfig, kubeClient, s.BusyboxImage, s.UseNodeProxyEndpoint)
	caClient, err := clusterclient.NewForConfig(kubeConfig)
	if err != nil {
		glog.Errorf("Failed to generate correct TAP config: %v", err.Error())
		caClient = nil
	}

	ormClient := resourcemapping.NewORMClient(dynamicClient, apiExtClient)

	// Configuration for creating the Kubeturbo TAP service
	vmtConfig := kubeturbo.NewVMTConfig2()
	vmtConfig.WithTapSpec(k8sTAPSpec).
		WithKubeClient(kubeClient).
		WithDynamicClient(dynamicClient).
		WithORMClient(ormClient).
		WithKubeletClient(kubeletClient).
		WithClusterAPIClient(caClient).
		WithVMPriority(s.VMPriority).
		WithVMIsBase(s.VMIsBase).
		UsingUUIDStitch(s.UseUUID).
		WithDiscoveryInterval(s.DiscoveryIntervalSec).
		WithValidationTimeout(s.ValidationTimeout).
		WithValidationWorkers(s.ValidationWorkers).
		WithDiscoveryWorkers(s.DiscoveryWorkers).
		WithDiscoveryTimeout(s.DiscoveryTimeoutSec).
		WithDiscoverySamples(s.DiscoverySamples).
		WithDiscoverySampleIntervalSec(s.DiscoverySampleIntervalSec).
		WithSccSupport(s.sccSupport).
		WithCAPINamespace(s.ClusterAPINamespace).
		WithContainerUtilizationDataAggStrategy(s.containerUtilizationDataAggStrategy).
		WithContainerUsageDataAggStrategy(s.containerUsageDataAggStrategy).
		WithVolumePodMoveConfig(s.FailVolumePodMoves)
	glog.V(3).Infof("Finished creating turbo configuration: %+v", vmtConfig)

	// The KubeTurbo TAP service
	k8sTAPService, err := kubeturbo.NewKubernetesTAPService(vmtConfig)
	if err != nil {
		glog.Fatalf("Unexpected error while creating Kubernetes TAP service: %s", err)
	}

	// Update scc resources in parallel.
	go manageSCCs(dynamicClient, kubeClient)

	// The client for healthz, debug, and prometheus
	go s.startHttp()

	cleanupWG := &sync.WaitGroup{}
	cleanupSCCFn := func() {
		cleanUpSCCMgmtResources(dynamicClient, kubeClient)
	}
	disconnectFn := func() {
		// Disconnect from Turbo server when Kubeturbo is shutdown
		// Close the mediation container including the endpoints. It avoids the
		// invalid endpoints remaining in the server side. See OM-28801.
		k8sTAPService.DisconnectFromTurbo()
	}
	handleExit(cleanupWG, cleanupSCCFn, disconnectFn)

	glog.V(1).Infof("********** Start running Kubeturbo Service **********")
	k8sTAPService.ConnectToTurbo()
	glog.V(1).Info("Kubeturbo service is stopped.")

	cleanupWG.Wait()
	glog.V(1).Info("Cleanup completed. Exiting gracefully.")
}

func (s *VMTServer) startHttp() {
	mux := http.NewServeMux()

	// healthz
	healthz.InstallHandler(mux)

	// debug
	if s.EnableProfiling {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

		// prometheus.metrics
		mux.Handle("/metrics", prometheus.Handler())
	}

	server := &http.Server{
		Addr:    net.JoinHostPort(s.Address, strconv.Itoa(s.Port)),
		Handler: mux,
	}
	glog.Fatal(server.ListenAndServe())
}

// handleExit disconnects the tap service from Turbo service when Kubeturbo is shotdown
func handleExit(wg *sync.WaitGroup, cleanUpFns ...cleanUp) { // k8sTAPService *kubeturbo.K8sTAPService) {
	glog.V(4).Infof("*** Handling Kubeturbo Termination ***")
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan,
		os.Interrupt,
		syscall.SIGTERM,
		syscall.SIGINT,
		syscall.SIGQUIT,
		syscall.SIGHUP)

	go func() {
		select {
		case sig := <-sigChan:
			glog.V(2).Infof("Signal %s received. Will run exit handlers.. \n", sig)
			for _, f := range cleanUpFns {
				// The default graceful timeout, once a container is sent a SIGTERM before it is
				// killed in k8s is 30 seconds. We want to make maximum use of that time.
				wg.Add(1)
				go func(f cleanUp) {
					f()
					wg.Done()
				}(f)
			}
		}
	}()
}

func discoverk8sAPIResourceGV(client *kubernetes.Clientset, resourceName string) (schema.GroupVersion, error) {
	// We optimistically use a globally set default if we cannot discover the GV.
	defaultGV := util.K8sAPIDeploymentReplicasetDefaultGV

	apiResourceLists, err := client.ServerPreferredResources()
	if apiResourceLists == nil {
		return defaultGV, err
	}
	if err != nil {
		// We don't exit here as ServerPreferredResources can return the resource list even with errors.
		glog.Warningf("Error listing api resources: %v", err)
	}

	latestExtensionsVersion := schema.GroupVersion{Group: util.K8sExtensionsGroupName, Version: ""}
	latestAppsVersion := schema.GroupVersion{Group: util.K8sAppsGroupName, Version: ""}
	for _, apiResourceList := range apiResourceLists {
		if len(apiResourceList.APIResources) == 0 {
			continue
		}

		found := false
		for _, apiResource := range apiResourceList.APIResources {
			if apiResource.Name == resourceName {
				found = true
				break
			}
		}
		if found == false {
			continue
		}

		gv, err := schema.ParseGroupVersion(apiResourceList.GroupVersion)
		if err != nil {
			return defaultGV, fmt.Errorf("error parsing GroupVersion: %v", err)
		}

		group := gv.Group
		version := gv.Version
		if group == util.K8sExtensionsGroupName {
			latestExtensionsVersion.Version = latestComparedVersion(version, latestExtensionsVersion.Version)
		} else if group == util.K8sAppsGroupName {
			latestAppsVersion.Version = latestComparedVersion(version, latestAppsVersion.Version)
		}
	}

	if latestAppsVersion.Version != "" {
		return latestAppsVersion, nil
	}
	if latestExtensionsVersion.Version != "" {
		return latestExtensionsVersion, nil
	}
	return defaultGV, nil
}

func latestComparedVersion(newVersion, existingVersion string) string {
	if existingVersion != "" && versionhelper.CompareKubeAwareVersionStrings(newVersion, existingVersion) <= 0 {
		return existingVersion
	}
	return newVersion
}

func manageSCCs(dynClient dynamic.Interface, kubeClient kubernetes.Interface) {
	// Its a must to include the namespace env var in the kubeturbo pod spec.
	ns := util.GetKubeturboNamespace()
	if reviewSCCAccess(ns, kubeClient) != true {
		// Skip managing scc resources; appropriate error messages are already logged
		// by the review function
		return
	}

	sccList := getSCCs(dynClient)
	if (sccList == nil) || (sccList != nil && len(sccList.Items) < 1) {
		// We don't need to bother as this cluster is most probably not openshift
		return
	}
	glog.V(3).Info("This looks like an openshift cluster and kubeturbo has appropriate permissions to manage SCCs.")

	fail := true
	defer func() {
		if fail {
			cleanUpSCCMgmtResources(dynClient, kubeClient)
		}
	}()

	saNames := []string{}
	for _, scc := range sccList.Items {
		sccName := scc.GetName()
		saName, err := createSCCServiceAccount(ns, sccName, kubeClient)
		if err != nil {
			// We have no option but to abort halfway and cleanup in case of any errors.
			// We already retry couple of times in case of an error.
			return
		}

		saNames = append(saNames, saName)

		err = addUserToSCC(userFullName(ns, saName), sccName, dynClient)
		if err != nil {
			glog.Errorf("Error adding scc user: %s to scc: %s: %v.", saName, sccName, err)
			return
		}

		// We use this map both for updating the user names in sccs and to cleanup the resources
		// in case of an error or at exit.
		// This has potential for race conditions, for example the service account was created
		// but not updated in this map when the exit was trigerred.
		// Ignoring this as of now because this can be no better then facing transient API errors
		// while deleting resources at exit, which will also leak resources behind.
		// Leaking resources is ok to some extent, because we use constant names and everything is
		// created within a namespace (except the updated user name in scc). Any leaked resources will
		// automatically be cleaned up when the kubeturbo namespace is deleted. The username updated in
		// the scc can potentially be left behind, but does not pose any security risk if the user
		// service account does not exist any more.
		util.SCCMapping[sccName] = saName
	}

	roleName, err := createSCCRole(ns, kubeClient)
	if err != nil {
		return
	}

	err = createSCCRoleBinding(saNames, ns, roleName, kubeClient)
	if err != nil {
		return
	}

	fail = false
}

func getSCCs(client dynamic.Interface) (sccList *unstructured.UnstructuredList) {
	res := schema.GroupVersionResource{
		Group:    util.OpenShiftAPISCCGV.Group,
		Version:  util.OpenShiftAPISCCGV.Version,
		Resource: util.OpenShiftSCCResName,
	}

	err := util.RetryDuring(util.TransientRetryTimes, 0,
		util.QuickRetryInterval, func() error {
			var err error
			sccList, err = client.Resource(res).List(metav1.ListOptions{})
			if err != nil {
				glog.Errorf("Failed to get openshift cluster sccs: %v", err)
			}
			return err
		})

	if err != nil {
		return nil
	}
	return sccList
}

func createSCCServiceAccount(namespace, sccName string, kubeClient kubernetes.Interface) (string, error) {
	sa := util.GetServiceAccountForSCC(sccName)
	saName := sa.Name

	// TODO: an improvement on retries would be to retry only on transient errors.
	err := util.RetryDuring(util.TransientRetryTimes, 0,
		util.QuickRetryInterval, func() error {
			_, err := kubeClient.CoreV1().ServiceAccounts(namespace).Create(sa)
			if apierrors.IsAlreadyExists(err) {
				glog.V(2).Infof("SCC ServiceAccount: %s/%s already exists.", namespace, saName)
				return nil
			}

			if err != nil {
				glog.Errorf("Error creating SCC ServicAccount: %s/%s, %s.", namespace, saName, err)
				return err
			}
			return nil

		})

	return saName, err
}

func createSCCRole(namespace string, kubeClient kubernetes.Interface) (string, error) {
	role := util.GetRoleForSCC()
	roleName := role.Name

	err := util.RetryDuring(util.TransientRetryTimes, 0,
		util.QuickRetryInterval, func() error {
			_, err := kubeClient.RbacV1().Roles(namespace).Create(role)
			if apierrors.IsAlreadyExists(err) {
				glog.V(3).Infof("SCC Role: %s/%s already exists.", namespace, roleName)
				return nil
			}

			if err != nil {
				glog.Errorf("Error creating SCC Role: %s/%s, %s.", namespace, roleName, err)
				return err
			}
			return nil
		})

	return roleName, err
}

func createSCCRoleBinding(saNames []string, namespace, roleName string, kubeClient kubernetes.Interface) error {
	rb := util.GetRoleBindingForSCC(saNames, namespace, roleName)
	return util.RetryDuring(util.TransientRetryTimes, 0,
		util.QuickRetryInterval, func() error {
			_, err := kubeClient.RbacV1().RoleBindings(namespace).Create(rb)
			if apierrors.IsAlreadyExists(err) {
				// We ignore the case where a new scc might appear between kubeturbo runs
				// That means a new scc definition will be picked across restarts.
				glog.V(3).Infof("SCC RoleBinding: %s/%s already exists.", rb.Namespace, rb.Name)
				return nil
			}

			if err != nil {
				glog.Errorf("Error creating SCC RoleBinding: %s/%s, %s.", rb.Namespace, rb.Name, err)
				return err
			}
			return nil
		})
}

func cleanUpSCCMgmtResources(dynClient dynamic.Interface, kubeClient kubernetes.Interface) {
	ns := util.GetKubeturboNamespace()
	if len(util.SCCMapping) < 1 {
		glog.V(2).Infof("SCC management resource cleanup is not needed.")
		return
	}
	glog.V(2).Infof("SCC management resource cleanup started.")

	for sccName, saName := range util.SCCMapping {
		err := removeUserFromSCC(userFullName(ns, saName), sccName, dynClient)
		if err != nil {
			glog.Errorf("Error removing sa(user): %s from scc: %s, %v", saName, sccName, err)
			// We still continue to try to cleanup rest of them.
		}

		err = util.RetryDuring(util.TransientRetryTimes, 0,
			util.QuickRetryInterval, func() error {
				deleteOptions := metav1.DeleteOptions{}
				err := kubeClient.CoreV1().ServiceAccounts(ns).Delete(saName, &deleteOptions)
				if apierrors.IsNotFound(err) {
					glog.V(2).Infof("SCC ServiceAccount: %s/%s already deleted.", ns, saName)
					return nil
				}

				if err != nil {
					glog.Errorf("Error deleting SCC ServicAccount: %s/%s, %s.", ns, saName, err)
					return err
				}

				return nil
			})
	}

	deleteSCCRoleBinding(ns, kubeClient)
	deleteSCCRole(ns, kubeClient)
	glog.V(2).Infof("SCC management resource cleanup completed.")
}

func deleteSCCRole(namespace string, kubeClient kubernetes.Interface) {
	roleName := util.SCCRoleName
	err := util.RetryDuring(util.TransientRetryTimes, 0,
		util.QuickRetryInterval, func() error {
			deleteOptions := metav1.DeleteOptions{}
			err := kubeClient.RbacV1().Roles(namespace).Delete(roleName, &deleteOptions)
			if apierrors.IsNotFound(err) {
				glog.V(3).Infof("SCC Role: %s/%s already deleted.", namespace, roleName)
				return nil
			}

			if err != nil {
				glog.Errorf("Error deleting SCC Role: %s/%s, %s.", namespace, roleName, err)
				return err
			}
			return nil
		})

	if err != nil {
		glog.Errorf("Error deleting SCC role.")
	}
}

func deleteSCCRoleBinding(namespace string, kubeClient kubernetes.Interface) {
	roleBindingName := util.SCCRoleBindingName
	err := util.RetryDuring(util.TransientRetryTimes, 0,
		util.QuickRetryInterval, func() error {
			deleteOptions := metav1.DeleteOptions{}
			err := kubeClient.RbacV1().RoleBindings(namespace).Delete(roleBindingName, &deleteOptions)
			if apierrors.IsNotFound(err) {
				glog.V(3).Infof("SCC RoleBinding: %s/%s already deleted.", namespace, roleBindingName)
			}

			if err != nil {
				glog.Errorf("Error deleting SCC RoleBinding: %s/%s, %s.", namespace, roleBindingName, err)
			}
			return nil
		})

	if err != nil {
		glog.Errorf("Error deleting SCC role binding.")
	}
}

// reviewSCCAccess checks the permissions for resources that are needed
// to be created or altered for SCC level functionality.
func reviewSCCAccess(namespace string, kubeClient kubernetes.Interface) bool {
	for _, review := range util.GetSelfSubjectAccessReviews(namespace) {
		permission, err := kubeClient.AuthorizationV1().SelfSubjectAccessReviews().Create(&review)
		if err != nil {
			glog.Errorf("Error reviewing kubeturbo permissions: %v. Kubeturbo cannot"+
				"use appropriate SCC levels while restarting pods.", err)
			return false
		}
		if permission.Status.Allowed != true {
			glog.Errorf("Kubeturbo does not have \"%s\" permission for \"%s\". Kubeturbo cannot"+
				"use appropriate SCC levels while restarting pods.", review.Spec.ResourceAttributes.Verb,
				review.Spec.ResourceAttributes.Resource)
			return false
		}
	}

	return true
}

func userFullName(ns, saName string) string {
	return fmt.Sprintf("system:serviceaccount:%s:%s", ns, saName)
}

func addUserToSCC(userFullName, sccName string, client dynamic.Interface) error {
	res := schema.GroupVersionResource{
		Group:    util.OpenShiftAPISCCGV.Group,
		Version:  util.OpenShiftAPISCCGV.Version,
		Resource: util.OpenShiftSCCResName,
	}

	var scc *unstructured.Unstructured
	err := util.RetryDuring(util.TransientRetryTimes, 0,
		util.QuickRetryInterval, func() error {
			var err error
			scc, err = client.Resource(res).Get(sccName, metav1.GetOptions{})
			if err != nil {
				// retry
				return err
			}
			return nil
		})

	users, ok, err := unstructured.NestedStringSlice(scc.Object, "users")
	if err != nil {
		return err
	}
	if !ok {
		// TODO: reverify this case, what can result in this not being found
	}
	if err != nil {
		return err
	}

	for _, user := range users {
		if userFullName == user {
			glog.Infof("SCC user %s already is in the user list of scc: %s.", userFullName, sccName)
			return nil
		}
	}

	users = append(users, userFullName)
	err = unstructured.SetNestedStringSlice(scc.Object, users, "users")
	if err != nil {
		return err
	}

	err = util.RetryDuring(util.TransientRetryTimes, 0,
		util.QuickRetryInterval, func() error {
			scc, err = client.Resource(res).Update(scc, metav1.UpdateOptions{})
			if err != nil {
				// retry
				return err
			}
			return nil
		})
	return err
}

// TODO: Remove code duplication
func removeUserFromSCC(userFullName, sccName string, client dynamic.Interface) error {
	res := schema.GroupVersionResource{
		Group:    util.OpenShiftAPISCCGV.Group,
		Version:  util.OpenShiftAPISCCGV.Version,
		Resource: util.OpenShiftSCCResName,
	}

	var scc *unstructured.Unstructured
	err := util.RetryDuring(util.TransientRetryTimes, 0,
		util.QuickRetryInterval, func() error {
			var err error
			scc, err = client.Resource(res).Get(sccName, metav1.GetOptions{})
			if err != nil {
				// retry
				return err
			}
			return nil
		})

	// Check if the user indeed is there in the list before deleting
	users, ok, err := unstructured.NestedStringSlice(scc.Object, "users")
	if err != nil {
		return err
	}
	if !ok || len(users) < 1 {
		// No users to remove
		return nil
	}
	if err != nil {
		return err
	}

	updatedUsers := []string{}
	found := false
	for _, user := range users {
		if userFullName == user {
			found = true
			continue
		}
		updatedUsers = append(updatedUsers, user)
	}

	if found == true {
		err := unstructured.SetNestedStringSlice(scc.Object, updatedUsers, "users")
		if err != nil {
			return err
		}

		err = util.RetryDuring(util.TransientRetryTimes, 0,
			util.QuickRetryInterval, func() error {
				scc, err = client.Resource(res).Update(scc, metav1.UpdateOptions{})
				if err != nil {
					// retry
					return err
				}
				return nil
			})
		return err
	} else {
		glog.Errorf("SCC user: %s does not exist in the scc: %s user list.", userFullName, sccName)
	}

	return nil
}
