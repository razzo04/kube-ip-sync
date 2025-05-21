import os
import sys
import yaml
import requests
import logging
from kubernetes import client, config
from kubernetes.client.rest import ApiException

# --- Logging Setup (same) ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# --- K8s API Method Mapping (for standard resources) ---
K8S_STANDARD_RESOURCE_API_MAP = {
    "service": {
        "client_class": client.CoreV1Api,
        "list_namespaced_method": "list_namespaced_service",
        "list_cluster_method": "list_service_for_all_namespaces",
        "read_namespaced_method": "read_namespaced_service",
        "patch_namespaced_method": "patch_namespaced_service",
        "read_namespaced_status_method": "read_namespaced_service_status", # Specific to Service
        "patch_namespaced_status_method": "patch_namespaced_service_status", # Specific to Service
    },
    "deployment": {
        "client_class": client.AppsV1Api,
        "list_namespaced_method": "list_namespaced_deployment",
        "list_cluster_method": "list_deployment_for_all_namespaces",
        "read_namespaced_method": "read_namespaced_deployment",
        "patch_namespaced_method": "patch_namespaced_deployment",
    },
    "ingress": {
        "client_class": client.NetworkingV1Api,
        "list_namespaced_method": "list_namespaced_ingress",
        "list_cluster_method": "list_ingress_for_all_namespaces",
        "read_namespaced_method": "read_namespaced_ingress",
        "patch_namespaced_method": "patch_namespaced_ingress",
    },
    # ... (add other standard kinds: StatefulSet, DaemonSet, Pod, ConfigMap, Secret etc.)
}

# --- Helper to get CRD info ---
def get_crd_params(kind, api_version_str):
    """
    Extracts group, version, and attempts to infer plural from kind.
    A more robust solution might require explicit 'plural' in config for CRDs.
    """
    if not api_version_str or '/' not in api_version_str:
        logging.error(f"Invalid apiVersion '{api_version_str}' for CR Kind '{kind}'. Expected 'group/version'.")
        return None, None, None
    group, version = api_version_str.split('/', 1)
    plural = kind.lower() + "s" # Common convention, but not always true
    # For CRDs not following the 's' plural convention, you'd ideally have 'plural' in config.
    # Example: kind: MyCustom, plural: mycustoms (default) vs plural: mycustomes (actual)
    logging.debug(f"CRD params for Kind '{kind}': group='{group}', version='{version}', plural='{plural}' (inferred)")
    return group, version, plural


# --- Configuration Loading (same) ---
def load_config(config_path):
    try:
        with open(config_path, "r") as f: return yaml.safe_load(f)
    except Exception as e:
        logging.error(f"Error loading configuration from {config_path}: {e}"); sys.exit(1)

# --- IP Management (same) ---
def get_public_ip(url): # (same)
    try:
        response = requests.get(url, timeout=10); response.raise_for_status()
        ip = response.text.strip()
        if not ip: raise ValueError("Received empty IP address")
        logging.info(f"Current public IP from {url}: {ip}"); return ip
    except Exception as e: logging.error(f"Error fetching/validating public IP from {url}: {e}"); return None

def get_last_known_ip(state_file_path): # (same)
    if not state_file_path: return None
    try:
        if os.path.exists(state_file_path):
            with open(state_file_path, "r") as f: return f.read().strip()
    except Exception as e: logging.warning(f"Could not read last known IP from {state_file_path}: {e}")
    return None

def store_last_known_ip(state_file_path, ip_address): # (same)
    if not state_file_path: return
    try:
        os.makedirs(os.path.dirname(state_file_path), exist_ok=True)
        with open(state_file_path, "w") as f: f.write(ip_address)
        logging.info(f"Stored current IP {ip_address} to {state_file_path}")
    except Exception as e: logging.error(f"Could not write last known IP to {state_file_path}: {e}")


# --- Kubernetes Resource Interaction ---
def update_annotation_on_object(k8s_object_meta, annotation_key, new_ip, patch_callback):
    """Generic annotation update logic given an object's metadata and a patch callback."""
    current_annotations = None
    if isinstance(k8s_object_meta, dict):
        # For CRs, k8s_object_meta is a dict
        current_annotations = k8s_object_meta.get("annotations")
    else:
        # For standard resources, k8s_object_meta is a model object (e.g., V1ObjectMeta)
        # The .annotations attribute itself can be None if no annotations exist.
        current_annotations = k8s_object_meta.annotations

    # Ensure current_annotations is a dictionary for consistent access,
    # even if it was None (no annotations field) or explicitly null.
    if current_annotations is None:
        current_annotations = {}

    if current_annotations.get(annotation_key) == new_ip:
        logging.info(f"Annotation '{annotation_key}' already set to '{new_ip}'. No update needed.")
        return True

    patch_body = {"metadata": {"annotations": {annotation_key: new_ip}}}
    try:
        patch_callback(patch_body)
        logging.info(f"Successfully updated annotation '{annotation_key}' to '{new_ip}'.")
        return True
    except ApiException as e:
        logging.error(f"K8s API error updating annotation: {e.body if e.body else e.reason}")
        return False
    except Exception as e:
        logging.error(f"Generic error updating annotation: {e}")
        return False

# update_service_loadbalancer_status remains largely the same, using CoreV1Api
def update_service_loadbalancer_status(core_v1_api, namespace, service_name, new_ip):
    # (Same as previous, ensure core_v1_api is passed correctly)
    logging.info(f"Attempting LoadBalancer status for Service/{service_name} in {namespace} to IP '{new_ip}'")
    service_api_methods = K8S_STANDARD_RESOURCE_API_MAP.get("service", {})
    try:
        read_status_method_name = service_api_methods.get("read_namespaced_status_method")
        if not read_status_method_name:
            logging.error("Service status methods not found in map for update_service_loadbalancer_status"); return False
        read_status_method = getattr(core_v1_api, read_status_method_name)
        service = read_status_method(service_name, namespace)

        if service.spec.type != "LoadBalancer":
            logging.info(f"Service {service_name} type is '{service.spec.type}', not LoadBalancer. Skipping.")
            return True

        current_lb_ingresses = service.status.load_balancer.ingress if service.status and service.status.load_balancer else []
        if current_lb_ingresses and current_lb_ingresses[0].ip == new_ip:
            logging.info(f"LoadBalancer IP for Service {service_name} already '{new_ip}'. No status update.")
            return True

        patch_body = {"status": {"loadBalancer": {"ingress": [{"ip": new_ip, "ipMode": "VIP"}]}}}
        patch_status_method_name = service_api_methods.get("patch_namespaced_status_method")
        if not patch_status_method_name:
            logging.error("Service status patch method not found in map"); return False

        patch_status_method = getattr(core_v1_api, patch_status_method_name)
        patch_status_method(service_name, namespace, patch_body)
        logging.info(f"Successfully patched LoadBalancer status for Service {service_name} to IP '{new_ip}'.")
        return True
    except ApiException as e:
        if e.status == 404: logging.error(f"Service {service_name} in {namespace} not found for status update.")
        else: logging.error(f"K8s API error updating LB status for {service_name}: {e.body if e.body else e.reason}")
        return False
    except Exception as e:
        logging.error(f"Error updating LB status for Service {service_name}: {e}")
        return False


# --- Annotation Selector Logic (same) ---
def matches_annotation_selector(resource_annotations, selector_config): # (same)
    if not selector_config: return True
    if not resource_annotations: resource_annotations = {}
    match_labels = selector_config.get("matchLabels", {})
    for key, value in match_labels.items():
        if resource_annotations.get(key) != value: return False
    match_expressions = selector_config.get("matchExpressions", [])
    for expr in match_expressions:
        key, operator, values = expr.get("key"), expr.get("operator"), expr.get("values", [])
        anno_val = resource_annotations.get(key)
        if operator == "Exists":
            if key not in resource_annotations: return False
        elif operator == "DoesNotExist":
            if key in resource_annotations: return False
        elif operator == "In":
            if anno_val is None or anno_val not in values: return False
        elif operator == "NotIn":
            if anno_val is not None and anno_val in values: return False
        elif operator == "Equals":
            if anno_val is None or anno_val not in values or len(values) != 1: return False
        else:
            logging.warning(f"Unsupported annotationSelector operator: {operator}"); return False
    return True


# --- Action Processor ---
def process_resource_actions(
    resource_details, # Dict containing name, namespace, kind, apiVersion (if CR), and the k8s_object itself
    actions_config, current_ip, default_anno_key,
    std_api_clients, custom_obj_api # Pass initialized API clients
):
    resource_success = True
    name = resource_details["name"]
    namespace = resource_details["namespace"]
    kind = resource_details["kind"]
    api_version = resource_details.get("apiVersion") # For CRs
    k8s_object = resource_details["k8s_object"] # The actual fetched Kubernetes object (dict for CRs)

    for action_item in actions_config:
        action_type = action_item.get("type")
        if action_type == "annotation":
            anno_key = action_item.get("annotationKey", default_anno_key)
            if not anno_key:
                logging.error(f"No annotationKey for {kind}/{name} and no default. Skipping.")
                resource_success = False; continue

            object_meta = k8s_object.get("metadata", {}) if isinstance(k8s_object, dict) else k8s_object.metadata

            def patch_callback(patch_body):
                if api_version: # It's a Custom Resource
                    group, cr_version, plural = get_crd_params(kind, api_version)
                    if not all([group, cr_version, plural]): resource_success = False; return # Error already logged
                    custom_obj_api.patch_namespaced_custom_object(group, cr_version, namespace, plural, name, patch_body)
                else: # Standard Resource
                    api_map_entry = K8S_STANDARD_RESOURCE_API_MAP.get(kind.lower())
                    if not api_map_entry:
                        logging.error(f"No API map entry for standard kind '{kind}'. Cannot patch."); resource_success = False; return
                    std_api_client = std_api_clients[kind.lower()]
                    patch_method = getattr(std_api_client, api_map_entry["patch_namespaced_method"])
                    patch_method(name, namespace, patch_body)

            if not update_annotation_on_object(object_meta, anno_key, current_ip, patch_callback):
                resource_success = False

        elif action_type == "loadBalancerStatus":
            if kind.lower() == "service" and not api_version: # Only for standard Service
                core_v1_api = std_api_clients.get("service")
                if not core_v1_api:
                     logging.error("CoreV1Api client not found for loadBalancerStatus update."); resource_success=False; continue
                if not update_service_loadbalancer_status(core_v1_api, namespace, name, current_ip):
                    resource_success = False
            else:
                logging.warning(f"Action 'loadBalancerStatus' is only for standard 'Service' kind. Skipping for {kind}/{name}.")
        else:
            logging.warning(f"Unknown action type '{action_type}' for {kind}/{name}. Skipping.")
    return resource_success


# --- Main Logic ---
def main():
    config_path = os.environ.get("CONFIG_FILE_PATH", "config.yaml")
    app_config = load_config(config_path)

    ip_check_url = app_config.get("ipCheckUrl")
    default_annotation_key = app_config.get("defaultAnnotationKey")
    last_ip_state_file = app_config.get("lastIpStateFile")

    if not ip_check_url: logging.error("ipCheckUrl not found."); sys.exit(1)

    try:
        if os.getenv("KUBERNETES_SERVICE_HOST"): config.load_incluster_config()
        else: config.load_kube_config()
    except config.ConfigException as e: logging.error(f"K8s client config error: {e}"); sys.exit(1)

    current_public_ip = get_public_ip(ip_check_url)
    if not current_public_ip: logging.error("Failed to get public IP."); sys.exit(1)

    last_known_ip = get_last_known_ip(last_ip_state_file)
    if last_known_ip == current_public_ip:
        logging.info(f"Public IP ({current_public_ip}) unchanged."); sys.exit(0)
    logging.info(f"Public IP changed: '{last_known_ip}' -> '{current_public_ip}' (or first run).")

    # Initialize API clients
    std_api_clients = {} # For standard resources
    all_std_kinds_in_config = set()
    for res_conf in app_config.get("resources", []) + app_config.get("resourceSelectors", []):
        if not res_conf.get("apiVersion"): # Standard resource if apiVersion is missing
            all_std_kinds_in_config.add(res_conf.get("kind","").lower())

    for kind_lower in all_std_kinds_in_config:
        if kind_lower and kind_lower in K8S_STANDARD_RESOURCE_API_MAP:
            api_class = K8S_STANDARD_RESOURCE_API_MAP[kind_lower]["client_class"]
            std_api_clients[kind_lower] = api_class()
        elif kind_lower:
            logging.warning(f"Standard Kind '{kind_lower}' in config but no API map entry. Will be skipped if encountered.")

    custom_obj_api = client.CustomObjectsApi() # Single client for all CRs

    overall_success = True
    updates_attempted = False

    resources_to_process = [] # A list of dicts: {name, namespace, kind, apiVersion, k8s_object, actions_config}

    # Gather individually specified resources
    for res_conf in app_config.get("resources", []):
        ns, kind, name = res_conf.get("namespace"), res_conf.get("kind"), res_conf.get("name")
        api_version = res_conf.get("apiVersion") # CR if present
        actions = res_conf.get("actions", [])

        if not all([ns, kind, name]):
            logging.warning(f"Skipping individual res with incomplete def: {res_conf}"); overall_success = False; continue

        try:
            k8s_object_data = None
            if api_version: # Custom Resource
                group, cr_version, plural = get_crd_params(kind, api_version)
                if not all([group, cr_version, plural]): overall_success = False; continue
                k8s_object_data = custom_obj_api.get_namespaced_custom_object(group, cr_version, ns, plural, name)
            else: # Standard Resource
                kind_lower = kind.lower()
                api_map_entry = K8S_STANDARD_RESOURCE_API_MAP.get(kind_lower)
                if not api_map_entry or kind_lower not in std_api_clients:
                    logging.error(f"No API map/client for std kind '{kind}' for individual res '{name}'."); overall_success = False; continue
                std_api_client = std_api_clients[kind_lower]
                read_method = getattr(std_api_client, api_map_entry["read_namespaced_method"])
                k8s_object_data = read_method(name, ns) # This returns a model object, not dict

            # For standard resources, convert model to dict for consistent metadata access if needed,
            # or handle model.metadata directly. Here, we'll pass the model.
            # For CRs, k8s_object_data is already a dict.
            resource_details = {
                "name": name, "namespace": ns, "kind": kind,
                "apiVersion": api_version, "k8s_object": k8s_object_data,
                "actions_config": actions
            }
            resources_to_process.append(resource_details)
        except ApiException as e:
            if e.status == 404: logging.warning(f"Individual resource {kind}/{name} in {ns} not found.")
            else: logging.error(f"API error fetching individual {kind}/{name}: {e.reason}"); overall_success = False
        except Exception as e:
            logging.error(f"Error fetching individual {kind}/{name}: {e}"); overall_success = False

    # Gather resources from selectors
    for selector_conf in app_config.get("resourceSelectors", []):
        kind = selector_conf.get("kind")
        api_version = selector_conf.get("apiVersion") # CR if present
        selector_ns = selector_conf.get("namespace")
        label_selector_str = selector_conf.get("labelSelector", "")
        field_selector_str = selector_conf.get("fieldSelector", "")
        annotation_selector_conf = selector_conf.get("annotationSelector")
        actions = selector_conf.get("actions", [])

        if not kind:
            logging.warning(f"Skipping selector with no Kind: {selector_conf}"); overall_success = False; continue

        fetched_items_list = [] # List of Kubernetes objects (models or dicts)
        try:
            if api_version: # Custom Resource
                group, cr_version, plural = get_crd_params(kind, api_version)
                if not all([group, cr_version, plural]): overall_success = False; continue
                if selector_ns:
                    response = custom_obj_api.list_namespaced_custom_object(
                        group, cr_version, selector_ns, plural,
                        label_selector=label_selector_str, field_selector=field_selector_str
                    )
                else:
                    response = custom_obj_api.list_cluster_custom_object(
                        group, cr_version, plural,
                        label_selector=label_selector_str, field_selector=field_selector_str
                    )
                fetched_items_list = response.get("items", []) # CR list returns a dict with 'items'
            else: # Standard Resource
                kind_lower = kind.lower()
                api_map_entry = K8S_STANDARD_RESOURCE_API_MAP.get(kind_lower)
                if not api_map_entry or kind_lower not in std_api_clients:
                    logging.error(f"No API map/client for std kind '{kind}' in selector."); overall_success = False; continue
                std_api_client = std_api_clients[kind_lower]
                list_method_name = api_map_entry["list_cluster_method"] if not selector_ns else api_map_entry["list_namespaced_method"]
                list_method = getattr(std_api_client, list_method_name)

                list_args = {"label_selector": label_selector_str, "field_selector": field_selector_str}
                if selector_ns: list_args["namespace"] = selector_ns

                response = list_method(**list_args)
                fetched_items_list = response.items # Standard list returns model with .items

            if not fetched_items_list:
                logging.info(f"No {kind} (apiVer: {api_version or 'core'}) found for selectors in ns '{selector_ns or 'all'}'.")

        except ApiException as e:
            logging.error(f"K8s API error listing {kind} (apiVer: {api_version or 'core'}) for selector: {e.reason}"); overall_success = False; continue
        except Exception as e:
            logging.error(f"Error listing {kind} (apiVer: {api_version or 'core'}) for selector: {e}"); overall_success = False; continue

        for item_obj in fetched_items_list:
            # CRs items are dicts, standard items are models. Access metadata accordingly.
            item_meta = item_obj.get("metadata") if isinstance(item_obj, dict) else item_obj.metadata
            item_annotations = item_meta.get("annotations") if isinstance(item_meta, dict) else item_meta.annotations

            if matches_annotation_selector(item_annotations, annotation_selector_conf):
                resource_details = {
                    "name": item_meta.get("name") if isinstance(item_meta, dict) else item_meta.name,
                    "namespace": item_meta.get("namespace") if isinstance(item_meta, dict) else item_meta.namespace,
                    "kind": kind, "apiVersion": api_version, "k8s_object": item_obj,
                    "actions_config": actions
                }
                resources_to_process.append(resource_details)
            else:
                item_name = item_meta.get("name") if isinstance(item_meta, dict) else item_meta.name
                item_ns_from_meta = item_meta.get("namespace") if isinstance(item_meta, dict) else item_meta.namespace
                logging.debug(f"Resource {kind}/{item_name} in {item_ns_from_meta} did not match annotationSelector.")

    # Process all gathered resources
    unique_resources_to_process = []
    seen_resources = set() # To avoid processing the same resource multiple times if matched by individual and selector

    for res_detail in resources_to_process:
        # Create a unique key for each resource: (namespace, kind, apiVersion or '', name)
        # apiVersion is important to differentiate between a standard 'Service' and a CR 'Service' if they had the same Kind name
        res_key = (
            res_detail["namespace"],
            res_detail["kind"],
            res_detail.get("apiVersion", ""), # Use empty string if not a CR for uniqueness
            res_detail["name"]
        )
        if res_key not in seen_resources:
            unique_resources_to_process.append(res_detail)
            seen_resources.add(res_key)
        else:
            logging.debug(f"Skipping duplicate processing for {res_detail['kind']}/{res_detail['name']} in {res_detail['namespace']}")


    for res_detail in unique_resources_to_process:
        updates_attempted = True
        logging.info(f"--- Processing actions for {res_detail['kind']}/{res_detail['name']} in {res_detail['namespace']} (apiVer: {res_detail.get('apiVersion','N/A')}) ---")
        if not process_resource_actions(res_detail, res_detail["actions_config"], current_public_ip, default_annotation_key, std_api_clients, custom_obj_api):
            overall_success = False

    if updates_attempted:
        if overall_success:
            logging.info("All attempted updates successful.")
            store_last_known_ip(last_ip_state_file, current_public_ip)
        else:
            logging.error("One or more updates failed. Not storing IP to force retry."); sys.exit(1)
    else:
        logging.info("No resources targeted or matched for update actions.")
        if last_known_ip != current_public_ip:
             store_last_known_ip(last_ip_state_file, current_public_ip)

    logging.info("IP update script finished.")

if __name__ == "__main__":
    main()