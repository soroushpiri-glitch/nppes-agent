import requests
import pandas as pd


def fetch_nppes_data(
    state=None,
    city=None,
    taxonomy_description=None,
    enumeration_type=None,
    first_name=None,
    last_name=None,
    organization_name=None,
    limit=200,
    max_skip=1000,
    verbose=True
):
    """
    Fetch NPPES data dynamically from the NPI Registry API.

    Parameters
    ----------
    state : str, optional
    city : str, optional
    taxonomy_description : str, optional
    enumeration_type : str, optional
        'NPI-1' for individuals, 'NPI-2' for organizations
    first_name : str, optional
    last_name : str, optional
    organization_name : str, optional
    limit : int, optional
        Max rows per request. Keep at or below 200.
    max_skip : int, optional
        Max skip allowed by API. Usually 1000.
    verbose : bool, optional
        Print progress.

    Returns
    -------
    all_results : list
        Raw JSON results from API
    """
    url = "https://npiregistry.cms.hhs.gov/api/"
    all_results = []
    skip = 0

    # Keep limit within API boundary
    if limit > 200:
        limit = 200

    while True:
        params = {
            "version": "2.1",
            "limit": limit,
            "skip": skip
        }

        # add only non-empty filters
        if state:
            params["state"] = state
        if city:
            params["city"] = city
        if taxonomy_description:
            params["taxonomy_description"] = taxonomy_description
        if enumeration_type:
            params["enumeration_type"] = enumeration_type
        if first_name:
            params["first_name"] = first_name
        if last_name:
            params["last_name"] = last_name
        if organization_name:
            params["organization_name"] = organization_name

        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
        except requests.RequestException as e:
            print(f"Request failed at skip={skip}: {e}")
            break

        results = data.get("results", [])

        if verbose:
            print(f"skip={skip}, got {len(results)} rows")

        if not results:
            break

        all_results.extend(results)
        skip += limit

        if skip > max_skip:
            if verbose:
                print("Reached API skip limit")
            break

    if verbose:
        print("Total collected:", len(all_results))

    return all_results


def nppes_results_to_dataframe(all_results):
    """
    Convert raw NPPES API results into a clean pandas DataFrame.
    """
    records = []

    for p in all_results:
        basic = p.get("basic", {})
        addresses = p.get("addresses", [])
        taxonomies = p.get("taxonomies", [])

        primary_tax = next((t for t in taxonomies if t.get("primary")), {})
        location = next((a for a in addresses if a.get("address_purpose") == "LOCATION"), {})

        # For NPI-2 organizations, organization name may live in basic["organization_name"]
        org_name = basic.get("organization_name")

        records.append({
            "NPI": p.get("number"),
            "Entity_Type": p.get("enumeration_type"),
            "First_Name": basic.get("first_name"),
            "Middle_Name": basic.get("middle_name"),
            "Last_Name": basic.get("last_name"),
            "Organization_Name": org_name,
            "Credential": basic.get("credential"),
            "Gender": basic.get("sex"),
            "Status": basic.get("status"),
            "Enumeration_Date": basic.get("enumeration_date"),
            "Last_Updated": basic.get("last_updated"),
            "Specialty": primary_tax.get("desc"),
            "Taxonomy_Code": primary_tax.get("code"),
            "City": location.get("city"),
            "State": location.get("state"),
            "Postal_Code": location.get("postal_code"),
            "Phone": location.get("telephone_number")
        })

    df = pd.DataFrame(records)
    return df
