import streamlit as st
from data import (
    build_polling_unit_results_df,
    get_states,
    get_lgas_by_state,
    get_wards_by_lga,
    filter_results,
    append_polling_unit_to_sql,
    add_polling_unit_to_df,
)


st.set_page_config(page_title="Bincom Survey", layout="centered")


@st.cache_data
def load_data():
    return build_polling_unit_results_df("bincom_test.sql")

DF = load_data()


def show_home():
    st.title("Ololade Ogunleye Bincom Online test interview 📝")
    st.markdown("""
    Welcome to the Bincom online test interview  app. Use the **sidebar** to navigate through pages.
    """)
    st.info("This app is a simple template Showing the data for Bincom Online test interview. It allows you to filter and view polling unit results based on different administrative levels (State, LGA, Ward, Polling Unit). You can also add new polling units to the dataset.")


def show_question_one():
    st.header("Individual Polling Unit Results")
    st.write("Click a button to select a level of location for the question.")

    # Ensure session state exists
    if "q1_selection" not in st.session_state:
        st.session_state.q1_selection = None

    cols = st.columns(4)

    if cols[0].button("State"):
        st.session_state.q1_selection = "State"
    if cols[1].button("LGA"):
        st.session_state.q1_selection = "LGA"
    if cols[2].button("Ward"):
        st.session_state.q1_selection = "Ward"
    if cols[3].button("Polling Unit"):
        st.session_state.q1_selection = "Polling Unit"

    if st.session_state.q1_selection:
        st.success(f"You selected: {st.session_state.q1_selection}")

        if st.session_state.q1_selection == "State":
            states = get_states(DF)
            if not states:
                st.warning("No state information available")
                return
            state_names = [name for (_id, name) in states]
            sel = st.selectbox("Choose a state", state_names)
            sid = next(_id for _id, name in states if name == sel)
            filtered = filter_results(DF, state_id=sid)
            st.write(f"{len(filtered)} polling units in {sel}")
            party_cols = [c for c in filtered.columns if c not in ['polling_unit_uniqueid','polling_unit_name','polling_unit_number','ward_id','lga_id','ward_name','lga_name','state_id','state_name']]
            if party_cols:
                totals = filtered[party_cols].sum().sort_values(ascending=False)
                st.dataframe(totals.to_frame('votes'))
                st.bar_chart(totals)

        elif st.session_state.q1_selection == "LGA":
            lgas = get_lgas_by_state(DF)
            if not lgas:
                st.warning("No LGA data available")
                return
            lga_names = [name for (_id, name) in lgas]
            sel = st.selectbox("Choose an LGA", lga_names)
            lid = next(_id for _id, name in lgas if name == sel)
            filtered = filter_results(DF, lga_id=lid)
            st.write(f"{len(filtered)} polling units in {sel}")
            party_cols = [c for c in filtered.columns if c not in ['polling_unit_uniqueid','polling_unit_name','polling_unit_number','ward_id','lga_id','ward_name','lga_name','state_id','state_name']]
            if party_cols:
                totals = filtered[party_cols].sum().sort_values(ascending=False)
                st.dataframe(totals.to_frame('votes'))
                st.bar_chart(totals)

        elif st.session_state.q1_selection == "Ward":
            # First select an LGA so wards are scoped to that LGA
            lgas = get_lgas_by_state(DF)
            if not lgas:
                st.warning("No LGA data available")
                return
            lga_names = [name for (_id, name) in lgas]
            sel_lga = st.selectbox("Choose an LGA", lga_names, key="q1_ward_lga")
            lid = next(_id for _id, name in lgas if name == sel_lga)

            wards = get_wards_by_lga(DF, lga_id=lid)
            if not wards:
                st.warning("No wards found for selected LGA")
                return
            ward_names = [name for (_id, name) in wards]
            sel = st.selectbox("Choose a ward", ward_names, key="q1_ward")
            wid = next(_id for _id, name in wards if name == sel)
            filtered = filter_results(DF, ward_id=wid)
            st.write(f"{len(filtered)} polling units in {sel}")
            party_cols = [c for c in filtered.columns if c not in ['polling_unit_uniqueid','polling_unit_name','polling_unit_number','ward_id','lga_id','ward_name','lga_name','state_id','state_name']]
            if party_cols:
                totals = filtered[party_cols].sum().sort_values(ascending=False)
                st.dataframe(totals.to_frame('votes'))
                st.bar_chart(totals)

        elif st.session_state.q1_selection == "Polling Unit":
            # Scope polling units by LGA then by Ward to make selection easier
            lgas = get_lgas_by_state(DF)
            if not lgas:
                st.warning("No LGA data available")
                return
            lga_names = [name for (_id, name) in lgas]
            sel_lga = st.selectbox("Choose an LGA", lga_names, key="q1_pu_lga")
            lid = next(_id for _id, name in lgas if name == sel_lga)

            wards = get_wards_by_lga(DF, lga_id=lid)
            if wards:
                ward_names = [name for (_id, name) in wards]
                sel_ward = st.selectbox("Choose a Ward (optional)", ["All Wards"] + ward_names, key="q1_pu_ward")
                if sel_ward != "All Wards":
                    wid = next(_id for _id, name in wards if name == sel_ward)
                    pus = DF[(DF['lga_id'] == lid) & (DF['ward_id'] == wid)][['polling_unit_uniqueid','polling_unit_name']].drop_duplicates().sort_values('polling_unit_name')
                else:
                    pus = DF[DF['lga_id'] == lid][['polling_unit_uniqueid','polling_unit_name']].drop_duplicates().sort_values('polling_unit_name')
            else:
                pus = DF[DF['lga_id'] == lid][['polling_unit_uniqueid','polling_unit_name']].drop_duplicates().sort_values('polling_unit_name')

            if pus.empty:
                st.warning("No polling units found for selected location")
                return

            sel_name = st.selectbox("Choose a polling unit", pus['polling_unit_name'].tolist(), key="q1_pu")
            pu_id = int(pus[pus['polling_unit_name'] == sel_name]['polling_unit_uniqueid'].iloc[0])
            filtered = filter_results(DF, polling_unit_id=pu_id)
            st.write(f"Results for {sel_name} (PU id {pu_id})")
            party_cols = [c for c in filtered.columns if c not in ['polling_unit_uniqueid','polling_unit_name','polling_unit_number','ward_id','lga_id','ward_name','lga_name','state_id','state_name']]
            if party_cols:
                totals = filtered[party_cols].sum().sort_values(ascending=False)
                st.dataframe(totals.to_frame('votes'))
                st.bar_chart(totals)


def show_question_two():
    st.header("Summed Polling Unit Results by LGA / State")
    st.write("Select a level for Question Two.")

    # Ensure session state exists for question two
    if "q2_selection" not in st.session_state:
        st.session_state.q2_selection = None

    cols = st.columns(2)
    if cols[0].button("State"):
        st.session_state.q2_selection = "State"
    if cols[1].button("LGA"):
        st.session_state.q2_selection = "LGA"

    if st.session_state.q2_selection:
        st.success(f"You selected: {st.session_state.q2_selection}")

        if st.session_state.q2_selection == "State":
            states = get_states(DF)
            state_names = [name for (_id, name) in states]
            sel = st.selectbox("Choose a state", state_names, key="q2_state")
            sid = next(_id for _id, name in states if name == sel)
            filtered = filter_results(DF, state_id=sid)
            st.write(f"Summed results for {sel} across {len(filtered)} polling units")
            party_cols = [c for c in filtered.columns if c not in ['polling_unit_uniqueid','polling_unit_name','polling_unit_number','ward_id','lga_id','ward_name','lga_name','state_id','state_name']]
            if party_cols:
                totals = filtered[party_cols].sum().sort_values(ascending=False)
                st.dataframe(totals.to_frame('votes'))
                st.bar_chart(totals)

        elif st.session_state.q2_selection == "LGA":
            lgas = get_lgas_by_state(DF)
            lga_names = [name for (_id, name) in lgas]
            sel = st.selectbox("Choose an LGA", lga_names, key="q2_lga")
            lid = next(_id for _id, name in lgas if name == sel)
            filtered = filter_results(DF, lga_id=lid)
            st.write(f"Summed results for {sel} across {len(filtered)} polling units")
            party_cols = [c for c in filtered.columns if c not in ['polling_unit_uniqueid','polling_unit_name','polling_unit_number','ward_id','lga_id','ward_name','lga_name','state_id','state_name']]
            if party_cols:
                totals = filtered[party_cols].sum().sort_values(ascending=False)
                st.dataframe(totals.to_frame('votes'))
                st.bar_chart(totals)


def show_question_three():
    st.header("Insert New Polling Unit 🧩")
    st.write("Add a new polling unit (State → LGA → Ward) and save it to the dataframe and SQL dump.")

    global DF

    states = get_states(DF)
    if not states:
        st.warning("No state/LGA/ward data available to add polling units.")
        return

    state_names = [name for (_id, name) in states]
    selected_state = st.selectbox("State", state_names)
    state_id = next(_id for _id, name in states if name == selected_state)

    lgas = get_lgas_by_state(DF, state_id=state_id)
    if not lgas:
        st.warning("No LGAs found for selected state")
        return
    lga_names = [name for (_id, name) in lgas]
    selected_lga = st.selectbox("LGA", lga_names)
    lga_id = next(_id for _id, name in lgas if name == selected_lga)

    wards = get_wards_by_lga(DF, lga_id=lga_id)
    if not wards:
        st.warning("No wards found for selected LGA")
        return
    ward_names = [name for (_id, name) in wards]
    selected_ward = st.selectbox("Ward", ward_names)
    ward_id = next(_id for _id, name in wards if name == selected_ward)

    pu_name = st.text_input("Polling Unit Name")
    pu_number = st.text_input("Polling Unit Number (optional)")

    # Show existing polling units for this (LGA, Ward) to avoid duplicates
    existing_pus = DF[(DF['lga_id'] == lga_id) & (DF['ward_id'] == ward_id)][['polling_unit_uniqueid', 'polling_unit_name', 'polling_unit_number']].drop_duplicates().sort_values('polling_unit_name')
    if not existing_pus.empty:
        st.markdown(f"**Existing polling units in {selected_ward}, {selected_lga} ({len(existing_pus)})**")
        st.dataframe(existing_pus.reset_index(drop=True))

    if st.button("Add Polling Unit"):
        # Create new unique id
        max_id = int(DF['polling_unit_uniqueid'].max()) if not DF['polling_unit_uniqueid'].isnull().all() else 0
        new_uid = max_id + 1

        # If user supplied a polling unit number, ensure it's unique within this ward/lga
        chosen_pu_number = pu_number or f'PU{new_uid}'
        if pu_number:
            dup = existing_pus[existing_pus['polling_unit_number'] == pu_number]
            if not dup.empty:
                st.error("A polling unit with that number already exists in the selected Ward/LGA. Choose a different number.")
                return

        # If user supplied a polling unit name, ensure it's unique within this ward/lga
        chosen_pu_name = pu_name or f'New PU {new_uid}'
        if pu_name:
            dupn = existing_pus[existing_pus['polling_unit_name'].str.lower() == pu_name.lower()]
            if not dupn.empty:
                st.error("A polling unit with that name already exists in the selected Ward/LGA. Choose a different name.")
                return

        # Prepare row dict
        pu_row = {
            'uniqueid': new_uid,
            'polling_unit_id': 0,
            'ward_id': int(ward_id),
            'lga_id': int(lga_id),
            'uniquewardid': 0,
            'polling_unit_number': chosen_pu_number,
            'polling_unit_name': chosen_pu_name,
            'polling_unit_description': '',
            'lat': None,
            'long': None,
            'entered_by_user': '',
            'date_entered': '0000-00-00 00:00:00',
            'user_ip_address': '',
            # extra helpers used for DF display
            'polling_unit_uniqueid': new_uid,
            'ward_name': selected_ward,
            'lga_name': selected_lga,
            'state_id': state_id,
            'state_name': selected_state,
        }

        # Append to SQL file
        sql_path = 'bincom_test.sql'
        try:
            stmt = append_polling_unit_to_sql(sql_path, pu_row)
        except Exception as e:
            st.error(f"Failed to write to SQL file: {e}")
            return

        # Update in-memory DF
        try:
            DF = add_polling_unit_to_df(DF, pu_row)
        except Exception as e:
            st.error(f"Failed to add polling unit to dataframe: {e}")
            return

        st.success(f"Polling unit '{pu_row['polling_unit_name']}' added (id={new_uid}).")
        st.code(stmt)


def main():
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Go to", ["Home", "Question One", "Question Two", "Question Three"])

    if page == "Home":
        show_home()
    elif page == "Question One":
        show_question_one()
    elif page == "Question Two":
        show_question_two()
    elif page == "Question Three":
        show_question_three()


if __name__ == "__main__":
    main()

