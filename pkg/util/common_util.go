package util

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	authv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/util/jsonpath"

	"github.com/golang/glog"
)

const (
	kubeturboNSEnvVar = "KUBETURBO_NAMESPACE"
	defaultNamespace  = "default"
	// We want these names to be unique and at the same time, we should
	// be able to identify these across restarts. We can also think of
	// making these user configurable, if need be.
	kubeturboSCCPrefix = "kubeturbo-scc-"
	SCCRoleName        = kubeturboSCCPrefix + "pod-restart-role"
	SCCRoleBindingName = kubeturboSCCPrefix + "pod-restart-rolebinding"
)

//  PatchValue specifies a patch operation for a string.
type PatchValue struct {
	Op    string      `json:"op"`
	Path  string      `json:"path"`
	Value interface{} `json:"value"`
}

// CompareVersion compares two version strings, for example:
// v1: "1.4.9",  v2: "1.5", then return -1
// v1: "1.5.0", v2: "1.5", then return 0
func CompareVersion(version1, version2 string) int {
	a1 := strings.Split(version1, ".")
	a2 := strings.Split(version2, ".")

	l1 := len(a1)
	l2 := len(a2)
	mlen := l1
	if mlen < l2 {
		mlen = l2
	}

	for i := 0; i < mlen; i++ {
		b1 := 0
		if i < l1 {
			if tmp, err := strconv.Atoi(a1[i]); err == nil {
				b1 = tmp
			}
		}

		b2 := 0
		if i < l2 {
			if tmp, err := strconv.Atoi(a2[i]); err == nil {
				b2 = tmp
			}
		}

		if b1 != b2 {
			return b1 - b2
		}
	}

	return 0
}

// RetryDuring executes a function with retries and a timeout
func RetryDuring(attempts int, timeout time.Duration, sleep time.Duration, myfunc func() error) error {
	t0 := time.Now()

	var err error
	for i := 0; ; i++ {
		if err = myfunc(); err == nil {
			// Log retries only after a failure
			if i > 0 {
				glog.V(4).Infof("[retry-%d/%d] success", i+1, attempts)
			}
			return nil
		}

		glog.V(4).Infof("[retry-%d/%d] Warning %v", i+1, attempts, err)
		if i >= (attempts - 1) {
			break
		}

		if timeout > 0 {
			if delta := time.Now().Sub(t0); delta > timeout {
				err = fmt.Errorf("failed after %d attepmts (during %v) last error: %v", i+1, delta, err)
				glog.Error(err)
				return err
			}
		}

		if sleep > 0 {
			time.Sleep(sleep)
		}
	}

	err = fmt.Errorf("failed after %d attepmts, last error: %v", attempts, err)
	glog.Error(err)
	return err
}

//RetrySimple executes a function with retries and a timeout
func RetrySimple(attempts int, timeout, sleep time.Duration, myfunc func() (bool, error)) error {
	t0 := time.Now()

	var err error
	for i := 0; ; i++ {
		retry := false
		if retry, err = myfunc(); !retry {
			return err
		}

		glog.V(4).Infof("[retry-%d/%d] Warning %v", i+1, attempts, err)
		if i >= (attempts - 1) {
			break
		}

		if timeout > 0 {
			if delta := time.Now().Sub(t0); delta > timeout {
				glog.Errorf("Failed after %d attepmts (during %v) last error: %v", i+1, delta, err)
				return err
			}
		}

		if sleep > 0 {
			time.Sleep(sleep)
		}
	}

	glog.Errorf("Failed after %d attepmts, last error: %v", attempts, err)
	return err
}

// NestedField returns the value of a nested field in the given object based on the given JSON-Path.
func NestedField(obj *unstructured.Unstructured, name, path string) (interface{}, bool, error) {
	j := jsonpath.New(name).AllowMissingKeys(true)
	template := fmt.Sprintf("{%s}", path)
	err := j.Parse(template)
	if err != nil {
		return nil, false, err
	}
	results, err := j.FindResults(obj.UnstructuredContent())
	if err != nil {
		return nil, false, err
	}
	if len(results) == 0 || len(results[0]) == 0 {
		return nil, false, nil
	}
	// The input path refers to a unique field, we can assume to have only one result or none.
	value := results[0][0].Interface()
	return value, true, nil
}

// JSONPath construct JSON-Path from given slice of fields.
func JSONPath(fields []string) string {
	return "." + strings.Join(fields, ".")
}

func GetKubeturboNamespace() string {
	namespace := os.Getenv(kubeturboNSEnvVar)
	if namespace == "" {
		namespace = defaultNamespace
	}
	return namespace
}

func GetRoleForSCC() *rbacv1.Role {
	return &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name: SCCRoleName,
		},
		Rules: []rbacv1.PolicyRule{
			{
				Verbs: []string{
					rbacv1.VerbAll,
				},
				APIGroups: []string{
					"",
				},
				Resources: []string{
					PodResName,
				},
			},
		},
	}
}

func GetRoleBindingForSCC(saNames []string, saNamespace, roleName string) *rbacv1.RoleBinding {
	subjects := []rbacv1.Subject{}
	for _, saName := range saNames {
		subjects = append(subjects, rbacv1.Subject{
			Kind:      rbacv1.ServiceAccountKind,
			Name:      saName,
			Namespace: saNamespace,
		})
	}

	return &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: SCCRoleBindingName,
		},
		Subjects: subjects,
		RoleRef: rbacv1.RoleRef{
			APIGroup: AuthorizationGroupName,
			Kind:     KindRole,
			Name:     roleName,
		},
	}
}

func GetServiceAccountForSCC(sccName string) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("%s%s", kubeturboSCCPrefix, sccName),
		},
	}
}

func GetSelfSubjectAccessReviews(namespace string) []authv1.SelfSubjectAccessReview {
	reviews := []authv1.SelfSubjectAccessReview{}

	// We need following permissions:
	// 1. edit sccs, to add the service account under the user section
	// 2. create service accounts, to create actual users to impersonate
	// 3. create role, to create a role allowing the above service account to be able to create pods
	// 4. create rolebinding, to attach the above role to the service accounts
	// 5. update rolebinding, with updated sa names in case the the above resources were leaked by
	// the last run of kubeturbo (for some possible unknown reason).
	// 6. impersonate, to be able to impersonate a service account

	// 1.
	r1 := authv1.ResourceAttributes{
		Group:     OpenShiftSecurityGroupName,
		Resource:  OpenShiftSCCResName,
		Verb:      VerbUpdate,
		Namespace: "",
	}
	reviews = append(reviews, authv1.SelfSubjectAccessReview{
		Spec: authv1.SelfSubjectAccessReviewSpec{
			ResourceAttributes: &r1,
		},
	})

	// 2.
	r2 := authv1.ResourceAttributes{
		Group:     "",
		Resource:  ServiceAccountResName,
		Verb:      VerbCreate,
		Namespace: namespace,
	}
	reviews = append(reviews, authv1.SelfSubjectAccessReview{
		Spec: authv1.SelfSubjectAccessReviewSpec{
			ResourceAttributes: &r2,
		},
	})

	// 3.
	r3 := authv1.ResourceAttributes{
		Group:     AuthorizationGroupName,
		Resource:  RoleResName,
		Verb:      VerbCreate,
		Namespace: namespace,
	}
	reviews = append(reviews, authv1.SelfSubjectAccessReview{
		Spec: authv1.SelfSubjectAccessReviewSpec{
			ResourceAttributes: &r3,
		},
	})

	// 4.
	r4 := authv1.ResourceAttributes{
		Group:     AuthorizationGroupName,
		Resource:  RoleBindingResName,
		Verb:      VerbCreate,
		Namespace: namespace,
	}
	reviews = append(reviews, authv1.SelfSubjectAccessReview{
		Spec: authv1.SelfSubjectAccessReviewSpec{
			ResourceAttributes: &r4,
		},
	})

	// 5.
	r5 := authv1.ResourceAttributes{
		Group:     AuthorizationGroupName,
		Resource:  RoleBindingResName,
		Verb:      VerbUpdate,
		Namespace: namespace,
	}
	reviews = append(reviews, authv1.SelfSubjectAccessReview{
		Spec: authv1.SelfSubjectAccessReviewSpec{
			ResourceAttributes: &r5,
		},
	})

	// 6.
	r6 := authv1.ResourceAttributes{
		Group:     AuthorizationGroupName,
		Resource:  ServiceAccountResName,
		Verb:      VerbImpersonate,
		Namespace: "",
	}
	reviews = append(reviews, authv1.SelfSubjectAccessReview{
		Spec: authv1.SelfSubjectAccessReviewSpec{
			ResourceAttributes: &r6,
		},
	})

	return reviews
}