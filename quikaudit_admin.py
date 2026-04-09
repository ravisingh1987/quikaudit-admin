import streamlit as st
from pymongo import MongoClient
import pymysql
from datetime import datetime
import pandas as pd

# ─── CONFIG ───────────────────────────────────────────────────────────────────

MONGODB_URI = st.secrets["MONGODB_URI"]
MARIADB_URI = st.secrets["MARIADB_URI"]
APP_PASSWORD = st.secrets["APP_PASSWORD"]

@st.cache_data(ttl=300)
def load_organizations():
    uri = st.secrets["MARIADB_URI"].replace("mysql+pymysql://", "")
    user_pass, rest = uri.split("@")
    user, password = user_pass.split(":")
    host_db = rest.split("/")
    host = host_db[0]
    db = host_db[1]
    conn = pymysql.connect(host=host, user=user, password=password, database=db, cursorclass=pymysql.cursors.DictCursor)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT org_id, name FROM organizations ORDER BY name")
            rows = cur.fetchall()
        return {r["name"]: r["org_id"] for r in rows}
    finally:
        conn.close()

ORGANIZATIONS = load_organizations()

COLLECTIONS = {
    "vendors":      {"field": "vendor",     "label": "Vendor Name",     "extra_fields": [{"key": "initials", "label": "Initials"}]},
    "colors":       {"field": "color",      "label": "Color Name",      "extra_fields": []},
    "fabric_types": {"field": "fabric_type","label": "Fabric Type",     "extra_fields": []},
    "parties":      {"field": "party",      "label": "Party Name",      "extra_fields": [{"key": "initials", "label": "Initials"}]},
    "job_workers":  {"field": "job_worker", "label": "Job Worker Name", "extra_fields": []},
    "merchants":    {"field": "merchant",   "label": "Merchant Name",   "extra_fields": []},
    "plans":        {"field": "plan",       "label": "Plan",            "extra_fields": []},
    "receivers":    {"field": "name",       "label": "Receiver Name",   "extra_fields": [{"key": "type", "label": "Type", "default": "printer"}]},
    "designs":      {"field": "design_no",  "label": "Design Number",   "extra_fields": []},
}

JOB_WORKER_TYPES = {"Outsource": 1, "In_House": 2}
JOB_WORKER_ROLES = {"Cutting": 1, "Printing": 2, "Embroidery": 3, "Stitching": 4, "Checking": 5, "Ironing": 6, "Packing": 7}
ADDRESS_TYPES = {"OFFICE": 1, "LOCAL": 2}
PHONE_TYPE_ID = 1
EMAIL_TYPE_ID = 1

# ─── DB CONNECTIONS ───────────────────────────────────────────────────────────

@st.cache_resource
def get_mongo_db():
    client = MongoClient(MONGODB_URI)
    return client["organization_db"]

def get_mariadb_conn():
    uri = MARIADB_URI.replace("mysql+pymysql://", "")
    user_pass, rest = uri.split("@")
    user, password = user_pass.split(":")
    host_db = rest.split("/")
    host = host_db[0]
    db = host_db[1]
    return pymysql.connect(host=host, user=user, password=password, database=db, cursorclass=pymysql.cursors.DictCursor)

def run_query(sql, params=None, fetch=True):
    conn = get_mariadb_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            if fetch:
                return cur.fetchall()
            else:
                conn.commit()
                return cur.rowcount
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def run_many(sql, data_list):
    conn = get_mariadb_conn()
    try:
        with conn.cursor() as cur:
            cur.executemany(sql, data_list)
            conn.commit()
            return cur.rowcount
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

# ─── PAGE ─────────────────────────────────────────────────────────────────────

st.set_page_config(page_title="QuikAudit Admin", page_icon="🧵", layout="wide")

# ─── AUTH ─────────────────────────────────────────────────────────────────────

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    st.title("🧵 QuikAudit Admin")
    st.markdown("Please enter the password to continue.")
    pwd = st.text_input("Password", type="password", key="login_pwd")
    if st.button("Login", type="primary"):
        if pwd == APP_PASSWORD:
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("Incorrect password.")
    st.stop()

st.title("🧵 QuikAudit Admin Panel")
st.caption("Manage master data for Thrive Fashion and Caesar Industries")

# ─── SIDEBAR ──────────────────────────────────────────────────────────────────

st.sidebar.header("Select Organisation")
org_name = st.sidebar.selectbox("Organisation", list(ORGANIZATIONS.keys()))
org_id = ORGANIZATIONS[org_name]
st.sidebar.markdown("---")
st.sidebar.markdown(f"**Org ID:**\n`{org_id}`")
st.sidebar.markdown("---")
if st.sidebar.button("🚪 Sign Out"):
    st.session_state.authenticated = False
    st.rerun()

# ─── MAIN TABS ────────────────────────────────────────────────────────────────

main_tab1, main_tab2, main_tab3 = st.tabs(["📦 MongoDB Master Data", "👷 Job Workers (MariaDB)", "🛠️ Support Tools"])

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1: MONGODB
# ═══════════════════════════════════════════════════════════════════════════════

with main_tab1:
    collection_name = st.selectbox("Collection", list(COLLECTIONS.keys()))
    col_config = COLLECTIONS[collection_name]
    main_field = col_config["field"]
    main_label = col_config["label"]
    extra_fields = col_config["extra_fields"]

    db = get_mongo_db()
    collection = db[collection_name]

    tab1, tab2, tab3 = st.tabs(["➕ Add Entry", "📋 View & Delete", "📥 Bulk Add"])

    with tab1:
        st.subheader(f"Add {main_label} for {org_name}")
        main_value = st.text_input(main_label, key="add_main")
        extra_values = {}
        for ef in extra_fields:
            extra_values[ef["key"]] = st.text_input(ef["label"], value=ef.get("default", ""), key=f"add_{ef['key']}")
        if st.button("Add Entry", type="primary", key="add_btn"):
            if not main_value.strip():
                st.error(f"{main_label} cannot be empty.")
            else:
                doc = {"organization_id": org_id, main_field: main_value.strip().lower()}
                for key, val in extra_values.items():
                    doc[key] = val.strip().lower() if val.strip() else ""
                collection.insert_one(doc)
                st.success(f"Added '{main_value}' to {collection_name} for {org_name}.")

    with tab2:
        st.subheader(f"All {collection_name} for {org_name}")
        docs = list(collection.find({"organization_id": org_id}, {"_id": 1, main_field: 1, **{ef["key"]: 1 for ef in extra_fields}}))
        if not docs:
            st.info("No entries found.")
        else:
            search = st.text_input("Search", placeholder=f"Filter by {main_label.lower()}...", key="search")
            filtered = [d for d in docs if not search or search.lower() in str(d.get(main_field, "")).lower()]
            st.caption(f"Showing {len(filtered)} of {len(docs)} entries")
            options = []
            id_map = {}
            for doc in filtered:
                display = doc.get(main_field, "")
                extras = " | ".join([f"{ef['key']}: {doc.get(ef['key'], '')}" for ef in extra_fields if doc.get(ef['key'])])
                label = f"{display}" + (f" [{extras}]" if extras else "")
                options.append(label)
                id_map[label] = doc["_id"]
            if options:
                selected = st.selectbox("Select entry to delete", options, key="delete_select")
                if st.button("Delete Selected", type="primary", key="delete_btn"):
                    collection.delete_one({"_id": id_map[selected]})
                    st.success(f"Deleted '{selected}'")
                    st.rerun()

    with tab3:
        st.subheader(f"Bulk Add {main_label} for {org_name}")
        if extra_fields:
            field_hint = ", ".join([ef["key"] for ef in extra_fields])
            st.info(f"Format: value,{field_hint} per line.")
        else:
            st.info(f"Paste one {main_label.lower()} per line.")
        bulk_text = st.text_area("Paste entries here", height=300, key="bulk_text")
        if st.button("Bulk Insert", type="primary", key="bulk_btn"):
            if not bulk_text.strip():
                st.error("Nothing to insert.")
            else:
                lines = [l.strip() for l in bulk_text.strip().splitlines() if l.strip()]
                existing_docs = collection.find({"organization_id": org_id}, {main_field: 1, "_id": 0})
                existing_values = set(d.get(main_field, "").lower() for d in existing_docs)
                docs_to_insert = []
                skipped = 0
                for line in lines:
                    parts = [p.strip() for p in line.split(",")]
                    main_val = parts[0].lower() if parts[0] else ""
                    if not main_val or main_val in existing_values:
                        skipped += 1
                        continue
                    doc = {"organization_id": org_id, main_field: main_val}
                    for i, ef in enumerate(extra_fields):
                        doc[ef["key"]] = parts[i+1].lower() if len(parts) > i+1 and parts[i+1] else ef.get("default", "").lower()
                    docs_to_insert.append(doc)
                    existing_values.add(main_val)
                if docs_to_insert:
                    try:
                        collection.insert_many(docs_to_insert, ordered=False)
                        st.success(f"Inserted: {len(docs_to_insert)} | Skipped: {skipped}")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
                else:
                    st.warning(f"Nothing new to insert. All {skipped} were duplicates or empty.")

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2: JOB WORKERS (MARIADB)
# ═══════════════════════════════════════════════════════════════════════════════

with main_tab2:
    st.subheader(f"Job Workers for {org_name}")
    jw_tab1, jw_tab2 = st.tabs(["➕ Add Job Worker", "📋 View Job Workers"])

    with jw_tab1:
        st.markdown("#### Basic Details")
        col1, col2 = st.columns(2)
        with col1:
            jw_name = st.text_input("Job Worker Name *", key="jw_name")
            jw_type = st.selectbox("Type *", list(JOB_WORKER_TYPES.keys()), key="jw_type")
            jw_role = st.selectbox("Role *", list(JOB_WORKER_ROLES.keys()), key="jw_role")
        with col2:
            jw_custom_id = st.text_input("Custom ID * (short unique code, no spaces e.g. MPNA)", key="jw_custom_id")
            jw_capacity = st.number_input("Capacity", min_value=0, value=0, key="jw_capacity")
            jw_gst = st.text_input("GST Number (optional)", key="jw_gst")
            jw_description = st.text_input("Description (optional)", key="jw_description")

        st.markdown("#### Contact Details (optional but appears on challan)")
        col3, col4 = st.columns(2)
        with col3:
            jw_phone = st.text_input("Phone Number", key="jw_phone")
        with col4:
            jw_email = st.text_input("Email", key="jw_email")

        st.markdown("#### Address (optional but appears on challan)")
        col5, col6 = st.columns(2)
        with col5:
            jw_addr_line1 = st.text_input("Address Line 1", key="jw_addr1")
            jw_addr_city = st.text_input("City", key="jw_city")
            jw_addr_country = st.text_input("Country", key="jw_country", value="India")
        with col6:
            jw_addr_line2 = st.text_input("Address Line 2 (optional)", key="jw_addr2")
            jw_addr_state = st.text_input("State", key="jw_state")
            jw_addr_pin = st.text_input("PIN Code", key="jw_pin")
            jw_addr_type = st.selectbox("Address Type", list(ADDRESS_TYPES.keys()), key="jw_addr_type")

        st.markdown("---")
        if st.button("Add Job Worker", type="primary", key="jw_add_btn"):
            if not jw_name.strip():
                st.error("Name is required.")
            elif not jw_custom_id.strip():
                st.error("Custom ID is required.")
            elif " " in jw_custom_id.strip():
                st.error("Custom ID cannot contain spaces. Use something like MPNA or MAPRNT.")
            else:
                try:
                    conn = get_mariadb_conn()
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT job_worker_id FROM job_workers WHERE custom_id = %s", (jw_custom_id.strip(),))
                        if cursor.fetchone():
                            st.error(f"Custom ID '{jw_custom_id}' already exists. Choose a different one.")
                        else:
                            now = datetime.utcnow()
                            cursor.execute("""
                                INSERT INTO job_workers (name, custom_id, type_id, role_id, capacity, gst, pin_code, description, created_at, updated_at)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, (
                                jw_name.strip(), jw_custom_id.strip(),
                                JOB_WORKER_TYPES[jw_type], JOB_WORKER_ROLES[jw_role],
                                jw_capacity,
                                jw_gst.strip() if jw_gst.strip() else None,
                                jw_addr_pin.strip() if jw_addr_pin.strip() else None,
                                jw_description.strip() if jw_description.strip() else None,
                                now, now
                            ))
                            new_id = cursor.lastrowid
                            cursor.execute("""
                                INSERT INTO job_worker_organization (job_worker_id, organization_id, assigned_at, is_active)
                                VALUES (%s, %s, %s, %s)
                            """, (new_id, org_id, now, 1))
                            if jw_phone.strip():
                                cursor.execute("""
                                    INSERT INTO job_worker_phones (job_worker_id, phone, type_id, created_at)
                                    VALUES (%s, %s, %s, %s)
                                """, (new_id, jw_phone.strip(), PHONE_TYPE_ID, now))
                            if jw_email.strip():
                                cursor.execute("""
                                    INSERT INTO job_worker_emails (job_worker_id, email, type_id, created_at)
                                    VALUES (%s, %s, %s, %s)
                                """, (new_id, jw_email.strip(), EMAIL_TYPE_ID, now))
                            if jw_addr_line1.strip():
                                cursor.execute("""
                                    INSERT INTO job_worker_addresses (job_worker_id, address_line1, address_line2, city, state, country, pin_code, type_id, created_at, updated_at)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """, (
                                    new_id, jw_addr_line1.strip(),
                                    jw_addr_line2.strip() if jw_addr_line2.strip() else None,
                                    jw_addr_city.strip() if jw_addr_city.strip() else None,
                                    jw_addr_state.strip() if jw_addr_state.strip() else None,
                                    jw_addr_country.strip() if jw_addr_country.strip() else None,
                                    jw_addr_pin.strip() if jw_addr_pin.strip() else None,
                                    ADDRESS_TYPES[jw_addr_type], now, now
                                ))
                            conn.commit()
                            st.success(f"✅ Job worker added successfully (ID: {new_id}) and linked to {org_name}.")
                            st.rerun()
                except Exception as e:
                    st.error(f"Database error: {str(e)}")
                finally:
                    conn.close()

    with jw_tab2:
        try:
            conn = get_mariadb_conn()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT jw.job_worker_id, jw.name, jw.custom_id,
                           jwt.type_name, jwr.role_name, jw.capacity,
                           jw.gst, jwo.is_active
                    FROM job_workers jw
                    JOIN job_worker_organization jwo ON jw.job_worker_id = jwo.job_worker_id
                    JOIN job_worker_type jwt ON jw.type_id = jwt.id
                    JOIN job_worker_role jwr ON jw.role_id = jwr.id
                    WHERE jwo.organization_id = %s
                    ORDER BY jw.job_worker_id DESC
                """, (org_id,))
                rows = cursor.fetchall()
            if not rows:
                st.info(f"No job workers found for {org_name}.")
            else:
                st.caption(f"{len(rows)} job workers found")
                df = pd.DataFrame(rows)
                df.columns = ["ID", "Name", "Custom ID", "Type", "Role", "Capacity", "GST", "Active"]
                df["Active"] = df["Active"].map({1: "✅ Yes", 0: "❌ No"})
                st.dataframe(df, use_container_width=True)
        except Exception as e:
            st.error(f"Database error: {str(e)}")
        finally:
            conn.close()

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3: SUPPORT TOOLS
# ═══════════════════════════════════════════════════════════════════════════════

with main_tab3:
    st.subheader(f"Support Tools — {org_name}")
    st.caption("Use these tools to resolve customer issues. All destructive actions require confirmation.")

    st_tab1, st_tab2, st_tab3, st_tab4 = st.tabs([
        "🗑️ Delete Design",
        "🔍 Diagnose Stuck Design",
        "✅ Complete Departments",
        "📦 Create Dispatch"
    ])

    # ── TOOL 1: DELETE DESIGN ─────────────────────────────────────────────────
    with st_tab1:
        st.markdown("#### Delete a Design")
        st.caption("Safely deletes a design and all linked records in the correct order.")

        design_search = st.text_input("Design Name or SO Number", placeholder="e.g. DP-124", key="del_search")

        if st.button("Search", key="del_search_btn") and design_search:
            results = run_query(
                "SELECT design_id, design_name, so_number, status, organization_id, design_create_date "
                "FROM designs WHERE (design_name LIKE %s OR so_number LIKE %s) AND organization_id = %s",
                (f"%{design_search}%", f"%{design_search}%", org_id)
            )
            if results:
                st.session_state["del_results"] = results
            else:
                st.error("No design found.")
                st.session_state.pop("del_results", None)

        if "del_results" in st.session_state:
            df = pd.DataFrame(st.session_state["del_results"])
            st.dataframe(df[["design_id", "design_name", "so_number", "status", "design_create_date"]], use_container_width=True)

            selected_id = st.selectbox(
                "Select Design to Delete",
                [r["design_id"] for r in st.session_state["del_results"]],
                format_func=lambda x: next(
                    f"{r['design_name']} | {r['status']} | {r['design_create_date']}"
                    for r in st.session_state["del_results"] if r["design_id"] == x
                ),
                key="del_select"
            )

            if selected_id:
                variants = run_query("SELECT COUNT(*) as cnt FROM product_variants WHERE design_id = %s", (selected_id,))
                fabric = run_query("SELECT COUNT(*) as cnt FROM fabric_transactions WHERE design_id = %s", (selected_id,))
                dept = run_query("SELECT COUNT(*) as cnt FROM design_department_tracking WHERE design_id = %s", (selected_id,))
                items = run_query(
                    "SELECT COUNT(*) as cnt FROM design_item_tracking WHERE tracking_id IN "
                    "(SELECT tracking_id FROM design_department_tracking WHERE design_id = %s)", (selected_id,)
                )

                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Variants", variants[0]["cnt"])
                col2.metric("Fabric Transactions", fabric[0]["cnt"])
                col3.metric("Dept Tracking", dept[0]["cnt"])
                col4.metric("Item Tracking", items[0]["cnt"])

                if fabric[0]["cnt"] > 0:
                    st.error("⛔ Cannot delete — fabric has been issued against this design. Deleting would corrupt inventory records.")
                else:
                    st.warning("⚠️ This action is permanent and cannot be undone.")
                    confirm_del = st.checkbox("I confirm I want to permanently delete this design", key="del_confirm")
                    if confirm_del and st.button("🗑️ Delete Design", type="primary", key="del_btn"):
                        try:
                            run_query(
                                "DELETE FROM design_item_tracking WHERE tracking_id IN "
                                "(SELECT tracking_id FROM design_department_tracking WHERE design_id = %s)",
                                (selected_id,), fetch=False
                            )
                            run_query("DELETE FROM design_department_tracking WHERE design_id = %s", (selected_id,), fetch=False)
                            run_query("DELETE FROM designs WHERE design_id = %s", (selected_id,), fetch=False)
                            st.success("✅ Design deleted successfully.")
                            st.session_state.pop("del_results", None)
                        except Exception as e:
                            st.error(f"Error: {e}")

    # ── TOOL 2: DIAGNOSE STUCK DESIGN ─────────────────────────────────────────
    with st_tab2:
        st.markdown("#### Diagnose Stuck Design")
        st.caption("Trace the full department journey and find exactly where a design is stuck.")

        diag_name = st.text_input("Design Name", placeholder="e.g. 1129", key="diag_name")

        if st.button("Diagnose", key="diag_btn") and diag_name:
            designs = run_query(
                "SELECT design_id, design_name, status, quantity FROM designs "
                "WHERE design_name = %s AND organization_id = %s",
                (diag_name, org_id)
            )
            if not designs:
                st.error("Design not found for this organisation.")
            else:
                st.session_state["diag_designs"] = designs

        if "diag_designs" in st.session_state:
            for design in st.session_state["diag_designs"]:
                st.markdown(f"**Design:** {design['design_name']} &nbsp;|&nbsp; **Status:** {design['status']} &nbsp;|&nbsp; **Order Qty:** {design['quantity']}")
                journey = run_query("""
                    SELECT
                        ddt.tracking_id,
                        dp.name as prev_dept,
                        dc.name as current_dept,
                        ddt.status_id,
                        ddt.processed_date,
                        SUM(dit.quantity) as total_qty
                    FROM design_department_tracking ddt
                    LEFT JOIN departments dp ON dp.department_id = ddt.prev_department_id
                    LEFT JOIN departments dc ON dc.department_id = ddt.current_department_id
                    LEFT JOIN design_item_tracking dit ON dit.tracking_id = ddt.tracking_id
                    WHERE ddt.design_id = %s
                    GROUP BY ddt.tracking_id, dp.name, dc.name, ddt.status_id, ddt.processed_date
                    ORDER BY ddt.processed_date ASC
                """, (design["design_id"],))

                if journey:
                    for step in journey:
                        icon = "✅" if step["status_id"] == 2 else "🔴"
                        label = "COMPLETED" if step["status_id"] == 2 else "IN PROGRESS"
                        st.markdown(
                            f"{icon} **{step['prev_dept'] or 'START'} → {step['current_dept']}** "
                            f"| Qty: `{step['total_qty']}` | `{label}` | "
                            f"<span style='color:gray;font-size:12px'>{step['processed_date']}</span>",
                            unsafe_allow_html=True
                        )

                    stuck = [s for s in journey if s["status_id"] == 1]
                    if stuck:
                        st.error(f"🔴 Stuck at {len(stuck)} department(s): " + ", ".join(s["current_dept"] for s in stuck))
                    else:
                        st.success("✅ All departments completed.")
                else:
                    st.info("No tracking records found.")

    # ── TOOL 3: BULK COMPLETE DEPARTMENTS ─────────────────────────────────────
    with st_tab3:
        st.markdown("#### Mark Departments as Completed")
        st.caption("Use when designs have physically completed a department but the app still shows them as IN PROGRESS.")

        bulk_names = st.text_area("Enter Design Names (one per line)", placeholder="1129\n1122\n1133", key="bulk_complete_names")

        if st.button("Find Stuck Records", key="bulk_find_btn") and bulk_names:
            names = [n.strip() for n in bulk_names.strip().splitlines() if n.strip()]
            placeholders = ", ".join(["%s"] * len(names))
            designs = run_query(
                f"SELECT design_id FROM designs WHERE design_name IN ({placeholders}) AND organization_id = %s",
                tuple(names) + (org_id,)
            )
            if not designs:
                st.error("No designs found.")
            else:
                design_ids = [d["design_id"] for d in designs]
                id_ph = ", ".join(["%s"] * len(design_ids))
                stuck = run_query(f"""
                    SELECT
                        d.design_name,
                        ddt.tracking_id,
                        dep.name as department_name,
                        SUM(dit.quantity) as total_qty
                    FROM design_department_tracking ddt
                    JOIN designs d ON d.design_id = ddt.design_id
                    JOIN departments dep ON dep.department_id = ddt.current_department_id
                    LEFT JOIN design_item_tracking dit ON dit.tracking_id = ddt.tracking_id
                    WHERE ddt.design_id IN ({id_ph}) AND ddt.status_id = 1
                    GROUP BY d.design_name, ddt.tracking_id, dep.name
                    ORDER BY d.design_name, ddt.tracking_id
                """, tuple(design_ids))

                if not stuck:
                    st.success("✅ No stuck records found. All departments already completed.")
                    st.session_state.pop("bulk_stuck", None)
                else:
                    st.session_state["bulk_stuck"] = stuck
                    st.session_state["bulk_stuck_ids"] = [s["tracking_id"] for s in stuck]
                    df = pd.DataFrame(stuck)
                    st.dataframe(df, use_container_width=True)
                    st.info(f"Found **{len(stuck)} stuck records** across {len(set(s['design_name'] for s in stuck))} designs.")

        if "bulk_stuck" in st.session_state:
            st.warning("⚠️ Only proceed if you've confirmed with the customer that these departments have physically completed their work.")
            confirm_bulk = st.checkbox("I confirm all these departments have physically completed their work", key="bulk_confirm")
            if confirm_bulk and st.button("✅ Mark All as Completed", type="primary", key="bulk_complete_btn"):
                try:
                    ids = st.session_state["bulk_stuck_ids"]
                    id_ph = ", ".join(["%s"] * len(ids))
                    affected = run_query(
                        f"UPDATE design_department_tracking SET status_id = 2 WHERE tracking_id IN ({id_ph})",
                        tuple(ids), fetch=False
                    )
                    st.success(f"✅ {affected} records marked as COMPLETED.")
                    st.session_state.pop("bulk_stuck", None)
                    st.session_state.pop("bulk_stuck_ids", None)
                except Exception as e:
                    st.error(f"Error: {e}")

    # ── TOOL 4: CREATE DISPATCH ────────────────────────────────────────────────
    with st_tab4:
        st.markdown("#### Create Dispatch Records")
        st.caption("Use when designs have been physically dispatched but the app was never updated.")

        dispatch_names = st.text_area("Enter Design Names (one per line)", placeholder="1129\n1122\n1133", key="dispatch_names")

        col1, col2 = st.columns(2)
        with col1:
            dispatch_date = st.date_input("Dispatch Date", value=datetime.today(), key="dispatch_date")
        with col2:
            dispatch_time = st.time_input("Dispatch Time", value=datetime.now().time(), key="dispatch_time")

        if st.button("Load Designs & Employees", key="dispatch_load_btn") and dispatch_names:
            names = [n.strip() for n in dispatch_names.strip().splitlines() if n.strip()]
            placeholders = ", ".join(["%s"] * len(names))
            designs = run_query(
                f"SELECT design_id, design_name, quantity FROM designs "
                f"WHERE design_name IN ({placeholders}) AND organization_id = %s",
                tuple(names) + (org_id,)
            )
            employees = run_query(
                "SELECT e.employee_id, u.full_name, e.role FROM employees e "
                "JOIN users u ON u.user_id = e.user_id "
                "WHERE e.organization_id = %s AND e.status = 'ONBOARDED' ORDER BY e.role",
                (org_id,)
            )
            if not designs:
                st.error("No designs found.")
            else:
                st.session_state["dispatch_designs"] = designs
                st.session_state["dispatch_employees"] = employees

        if "dispatch_designs" in st.session_state and "dispatch_employees" in st.session_state:
            emp_options = {
                e["employee_id"]: f"{e['full_name']} ({e['role']})"
                for e in st.session_state["dispatch_employees"]
            }

            col1, col2 = st.columns(2)
            with col1:
                dispatched_by = st.selectbox("Dispatched By", list(emp_options.keys()), format_func=lambda x: emp_options[x], key="disp_by")
            with col2:
                approved_by = st.selectbox("Approved By", list(emp_options.keys()), format_func=lambda x: emp_options[x], key="disp_approved")

            df = pd.DataFrame(st.session_state["dispatch_designs"])
            st.dataframe(df, use_container_width=True)

            # Check for existing dispatch records
            design_ids = [d["design_id"] for d in st.session_state["dispatch_designs"]]
            id_ph = ", ".join(["%s"] * len(design_ids))
            existing = run_query(f"SELECT design_id FROM dispatch_records WHERE design_id IN ({id_ph})", tuple(design_ids))
            existing_ids = {e["design_id"] for e in existing}
            new_designs = [d for d in st.session_state["dispatch_designs"] if d["design_id"] not in existing_ids]

            if existing_ids:
                st.warning(f"⚠️ {len(existing_ids)} design(s) already have dispatch records and will be skipped.")
            if not new_designs:
                st.error("All selected designs already have dispatch records.")
            else:
                st.info(f"ℹ️ Will create dispatch records for {len(new_designs)} design(s). Quantities pulled from Cutting department.")
                confirm_dispatch = st.checkbox("I confirm these designs have been physically dispatched", key="dispatch_confirm")

                if confirm_dispatch and st.button("📦 Create Dispatch Records", type="primary", key="dispatch_create_btn"):
                    try:
                        dispatch_dt = datetime.combine(dispatch_date, dispatch_time).strftime("%Y-%m-%d %H:%M:%S")
                        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                        cutting_dept = run_query(
                            "SELECT department_id FROM departments WHERE org_id = %s AND department_type = 'Cutting' AND is_active = 1 LIMIT 1",
                            (org_id,)
                        )
                        if not cutting_dept:
                            st.error("No active Cutting department found for this organisation.")
                            st.stop()
                        cutting_dept_id = cutting_dept[0]["department_id"]

                        success_count = 0
                        for design in new_designs:
                            did = design["design_id"]

                            last_tracking = run_query(
                                "SELECT tracking_id FROM design_department_tracking WHERE design_id = %s ORDER BY processed_date DESC LIMIT 1",
                                (did,)
                            )
                            if not last_tracking:
                                continue
                            tracking_id = last_tracking[0]["tracking_id"]

                            run_query(
                                "INSERT INTO dispatch_records (design_id, tracking_id, dispatched_by, dispatch_date, "
                                "approval_status_id, approved_by, approval_date, created_at, party_id) "
                                "VALUES (%s, %s, %s, %s, 1, %s, %s, %s, NULL)",
                                (did, tracking_id, dispatched_by, dispatch_dt, approved_by, dispatch_dt, now),
                                fetch=False
                            )

                            dispatch_rec = run_query(
                                "SELECT dispatch_id FROM dispatch_records WHERE design_id = %s AND dispatched_by = %s ORDER BY dispatch_id DESC LIMIT 1",
                                (did, dispatched_by)
                            )
                            dispatch_id = dispatch_rec[0]["dispatch_id"]

                            cutting_variants = run_query("""
                                SELECT pv.variant_id, dit.quantity
                                FROM design_department_tracking ddt
                                JOIN design_item_tracking dit ON dit.tracking_id = ddt.tracking_id
                                JOIN product_variants pv ON pv.variant_id = dit.variant_id
                                WHERE ddt.design_id = %s AND ddt.current_department_id = %s
                            """, (did, cutting_dept_id))

                            if cutting_variants:
                                item_data = [(dispatch_id, v["variant_id"], v["quantity"], now) for v in cutting_variants]
                                run_many(
                                    "INSERT INTO dispatch_items (dispatch_id, variant_id, quantity, created_at) VALUES (%s, %s, %s, %s)",
                                    item_data
                                )
                            success_count += 1

                        st.success(f"✅ Dispatch records created for {success_count} design(s).")
                        st.session_state.pop("dispatch_designs", None)
                        st.session_state.pop("dispatch_employees", None)

                    except Exception as e:
                        st.error(f"Error: {e}")
