from dataclasses import dataclass


@dataclass
class RawData:
    EXCEL_PATH = "src/Data/Raw_Data_Links.xlsx"
    SHEET_NAME_CSJ_VIDS = "CSJVids"
    SHEET_NAME_RP_VIDS = "RPVids"
    SHEET_NAME_CSJ_BLOG = "CSJBlog"
    SHEET_NAME_RP_BLOG = "RPBlog"
    RAW_CSJ_FREE = "src/Data/1_Raw/CSJ/Free"
    RAW_CSJ_PAID = "src/Data/1_Raw/CSJ/Paid"
    RAW_RP_FREE = "src/Data/1_Raw/RP/Free"
    RAW_RP_PAID = "src/Data/1_Raw/RP/Paid"
    RAW_CSJ_BLOG = "src/Data/1_Raw/CSJ/Blog"
    RAW_RP_BLOG = "src/Data/1_Raw/RP/Blog"


@dataclass
class BlogJSONSchema:
    JS_WAIT_TIME = """await new Promise(r=>setTimeout(r,5000));"""
    PAGINATION_WORD = "Older Entries"
    SCHEMA_CSJ_BLOG_INIT = {
        "name": "Initial Blog Links via CSS",
        "baseSelector": "#left-area",
        "fields": [
            {
                "name": "articles",
                "selector": "article",
                "type": "nested_list",
                "fields": [
                    {
                        "name": "video_name",
                        "selector": "h2.entry-title a",
                        "type": "text",
                    },
                    {
                        "name": "video_link",
                        "selector": "h2.entry-title a",
                        "type": "attribute",
                        "attribute": "href",
                    },
                ],
            },
        ],
    }

    SCHEMA_CSJ_BLOG_PAGE = {
        "name": "Transcript schema via CSS",
        "baseSelector": "#et-main-area > #main-content",
        "fields": [
            # {
            #     "name": "video_name",
            #     "selector": "div.et_post_meta_wrapper > h1.entry-title",
            #     "type": "text",
            # },
            {
                "name": "transcript",
                "selector": "div.et_pb_section.et_pb_section_1 > div.et_pb_row.et_pb_row_1 > div.et_pb_column.et_pb_column_4_4",
                "type": "text",
                "default": "",
            },
        ],
    }
