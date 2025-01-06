from useful import file_upload, interactive_widgets, progress_bar, session_state, forms
from streamlit_option_menu import option_menu

selected_main = option_menu(
    menu_title=None,
    options=["Home", "File Upload", "Interactive Widgets", "Progress Bar", "Session State", "Forms"],
    icons=["house", "upload", "sliders", "hourglass-split", "key", "file-text"],
    menu_icon="cast",
    default_index=0,
    orientation="horizontal",
)

if selected_main == "File Upload":
    file_upload.show()
elif selected_main == "Interactive Widgets":
    interactive_widgets.show()
elif selected_main == "Progress Bar":
    progress_bar.show()
elif selected_main == "Session State":
    session_state.show()
elif selected_main == "Forms":
    forms.show()