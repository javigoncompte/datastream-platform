Below is a complete API reference for `MonsterUI`.  If it is not here it does not exist in monsterui.  However you can build your own components using tailwind if you must, though it is much preferred and idiomatic to use `MonsterUI`

## monsterui.core

- `class ThemeRadii(Enum)`
    Members: none, sm, md, lg

- `class ThemeShadows`

- `class ThemeFont`

- `class Theme(Enum)`
    Selector to choose theme and get all headers needed for app.  Includes frankenui + tailwind + daisyui + highlight.js options
    Members: slate, stone, gray, neutral, red, rose, orange, green, blue, yellow, violet, zinc

    - `headers(self, mode, daisy, highlightjs, katex, radii, shadows, font)`
        Create frankenui and tailwind cdns

    - `local_headers(self, mode, static_dir, daisy, highlightjs, katex, radii, shadows, font)`
        Create headers using local files downloaded from CDNs


## monsterui.daisy

- `class AlertT(Enum)`
    Alert styles from DaisyUI
    Members: info, success, warning, error


- `def Alert(*c, **kwargs)`
    Alert informs users about important events.

- `class StepsT(Enum)`
    Options for Steps
    Members: vertical, horizonal


- `class StepT(Enum)`
    Step styles for LiStep
    Members: primary, secondary, accent, info, success, warning, error, neutral


- `def Steps(*li, **kwargs)`
    Creates a steps container

- `def LiStep(*c, **kwargs)`
    Creates a step list item

- `class LoadingT(Enum)`
    Members: spinner, dots, ring, ball, bars, infinity, xs, sm, md, lg


- `def Loading(cls, htmx_indicator, **kwargs)`
    Creates a loading animation component

- `class ToastHT(Enum)`
    Horizontal position for Toast
    Members: start, center, end


- `class ToastVT(Enum)`
    Vertical position for Toast
    Members: top, middle, bottom


- `def Toast(*c, **kwargs)`
    Toasts are stacked announcements, positioned on the corner of page.

## monsterui.foundations

> Data Structures and Utilties

- `def stringify(o)`
    Converts input types into strings that can be passed to FT components

- `class VEnum(Enum)`
    Members: 

    - `__str__(self)`
    - `__add__(self, other)`
    - `__radd__(self, other)`

## monsterui.franken

- `class TextT(Enum)`
    Text Styles from https://franken-ui.dev/docs/text
    Members: paragraph, lead, meta, gray, italic, xs, sm, lg, xl, light, normal, medium, bold, extrabold, muted, primary, secondary, success, warning, error, info, left, right, center, justify, start, end, top, middle, bottom, truncate, break_, nowrap, underline, highlight


- `class TextPresets(Enum)`
    Common Typography Presets
    Members: muted_sm, muted_lg, bold_sm, bold_lg, md_weight_sm, md_weight_muted


- `def CodeSpan(*c, **kwargs)`
    A CodeSpan with Styling

- `def CodeBlock(*c, **kwargs)`
    CodeBlock with Styling

- `def H1(*c, **kwargs)`
    H1 with styling and appropriate size

- `def H2(*c, **kwargs)`
    H2 with styling and appropriate size

- `def H3(*c, **kwargs)`
    H3 with styling and appropriate size

- `def H4(*c, **kwargs)`
    H4 with styling and appropriate size

- `def H5(*c, **kwargs)`
    H5 with styling and appropriate size

- `def H6(*c, **kwargs)`
    H6 with styling and appropriate size

- `def Subtitle(*c, **kwargs)`
    Styled muted_sm text designed to go under Headings and Titles

- `def Q(*c, **kwargs)`
    Styled quotation mark

- `def Em(*c, **kwargs)`
    Styled emphasis text

- `def Strong(*c, **kwargs)`
    Styled strong text

- `def I(*c, **kwargs)`
    Styled italic text

- `def Small(*c, **kwargs)`
    Styled small text

- `def Mark(*c, **kwargs)`
    Styled highlighted text

- `def Del(*c, **kwargs)`
    Styled deleted text

- `def Ins(*c, **kwargs)`
    Styled inserted text

- `def Sub(*c, **kwargs)`
    Styled subscript text

- `def Sup(*c, **kwargs)`
    Styled superscript text

- `def Blockquote(*c, **kwargs)`
    Blockquote with Styling

- `def Caption(*c, **kwargs)`
    Styled caption text

- `def Cite(*c, **kwargs)`
    Styled citation text

- `def Time(*c, **kwargs)`
    Styled time element

- `def Address(*c, **kwargs)`
    Styled address element

- `def Abbr(*c, **kwargs)`
    Styled abbreviation with dotted underline

- `def Dfn(*c, **kwargs)`
    Styled definition term with italic and medium weight

- `def Kbd(*c, **kwargs)`
    Styled keyboard input with subtle background

- `def Samp(*c, **kwargs)`
    Styled sample output with subtle background

- `def Var(*c, **kwargs)`
    Styled variable with italic monospace

- `def Figure(*c, **kwargs)`
    Styled figure container with card-like appearance

- `def Details(*c, **kwargs)`
    Styled details element

- `def Summary(*c, **kwargs)`
    Styled summary element

- `def Data(*c, **kwargs)`
    Styled data element

- `def Meter(*c, **kwargs)`
    Styled meter element

- `def S(*c, **kwargs)`
    Styled strikethrough text (different semantic meaning from Del)

- `def U(*c, **kwargs)`
    Styled underline (for proper names in Chinese, proper spelling etc)

- `def Output(*c, **kwargs)`
    Styled output element for form results

- `def PicSumImg(h, w, id, grayscale, blur, **kwargs)`
    Creates a placeholder image using https://picsum.photos/

- `class ButtonT(Enum)`
    Options for styling Buttons
    Members: default, ghost, primary, secondary, destructive, text, link, xs, sm, lg, xl, icon


- `def Button(*c, **kwargs)`
    Button with Styling (defaults to `submit` for form submission)

- `class ContainerT(Enum)`
    Max width container sizes from https://franken-ui.dev/docs/container
    Members: xs, sm, lg, xl, expand


- `class BackgroundT(Enum)`
    Members: muted, primary, secondary, default


- `def Container(*c, **kwargs)`
    Div to be used as a container that often wraps large sections or a page of content

- `def Titled(title, *c, **kwargs)`
    Creates a standard page structure for titled page.  Main(Container(title, content))

- `class DividerT(Enum)`
    Divider Styles from https://franken-ui.dev/docs/divider
    Members: icon, sm, vertical


- `def Divider(*c, **kwargs)`
    Divider with default styling and margin

- `def DividerSplit(*c)`
    Creates a simple horizontal line divider with configurable thickness and vertical spacing

- `def Article(*c, **kwargs)`
    A styled article container for blog posts or similar content

- `def ArticleTitle(*c, **kwargs)`
    A title component for use within an Article

- `def ArticleMeta(*c, **kwargs)`
    A metadata component for use within an Article showing things like date, author etc

- `class SectionT(Enum)`
    Section styles from https://franken-ui.dev/docs/section
    Members: default, muted, primary, secondary, xs, sm, lg, xl, remove_vertical


- `def Section(*c, **kwargs)`
    Section with styling and margins

- `def Form(*c, **kwargs)`
    A Form with default spacing between form elements

- `def Fieldset(*c, **kwargs)`
    A Fieldset with default styling

- `def Legend(*c, **kwargs)`
    A Legend with default styling

- `def Input(*c, **kwargs)`
    An Input with default styling

- `def Radio(*c, **kwargs)`
    A Radio with default styling

- `def CheckboxX(*c, **kwargs)`
    A Checkbox with default styling

- `def Range(*c, **kwargs)`
    A Range with default styling

- `def TextArea(*c, **kwargs)`
    A Textarea with default styling

- `def Switch(*c, **kwargs)`
    A Switch with default styling

- `def Upload(*c, **kwargs)`
    A file upload component with default styling

- `def UploadZone(*c, **kwargs)`
    A file drop zone component with default styling

- `def FormLabel(*c, **kwargs)`
    A Label with default styling

- `class LabelT(Enum)`
    Members: primary, secondary, danger


- `def Label(*c, **kwargs)`
    FrankenUI labels, which look like pills

- `def UkFormSection(title, description, *c)`
    A form section with a title, description and optional button

- `def GenericLabelInput(label, lbl_cls, input_cls, container, cls, id, input_fn, **kwargs)`
    `Div(Label,Input)` component with Uk styling injected appropriately. Generally you should higher level API, such as `LabelInput` which is created for you in this library

- `def LabelInput(label, lbl_cls, input_cls, cls, id, **kwargs)`
    A `FormLabel` and `Input` pair that provides default spacing and links/names them based on id

- `def LabelRadio(label, lbl_cls, input_cls, container, cls, id, **kwargs)`
    A FormLabel and Radio pair that provides default spacing and links/names them based on id

- `def LabelCheckboxX(label, lbl_cls, input_cls, container, cls, id, **kwargs)`
    A FormLabel and CheckboxX pair that provides default spacing and links/names them based on id

- `@delegates(GenericLabelInput, but=['input_fn', 'cls']) def LabelRange(label, lbl_cls, input_cls, cls, id, value, min, max, step, label_range, **kwargs)`
    A FormLabel and Range pair that provides default spacing and links/names them based on id

- `class AT(Enum)`
    Link styles from https://franken-ui.dev/docs/link
    Members: muted, text, reset, primary, classic

- `class ListT(Enum)`
    List styles using Tailwind CSS
    Members: disc, circle, square, decimal, hyphen, bullet, divider, striped


- `def ModalContainer(*c, **kwargs)`
    Creates a modal container that components go in

- `def ModalDialog(*c, **kwargs)`
    Creates a modal dialog

- `def ModalHeader(*c, **kwargs)`
    Creates a modal header

- `def ModalBody(*c, **kwargs)`
    Creates a modal body

- `def ModalFooter(*c, **kwargs)`
    Creates a modal footer

- `def ModalTitle(*c, **kwargs)`
    Creates a modal title

- `def ModalCloseButton(*c, **kwargs)`
    Creates a button that closes a modal with js

- `def Modal(*c, **kwargs)`
    Creates a modal with the appropriate classes to put the boilerplate in the appropriate places for you

- `def Placeholder(*c, **kwargs)`
    Creates a placeholder

- `def Progress(*c, **kwargs)`
    Creates a progress bar

- `def UkIcon(icon, height, width, stroke_width, cls, **kwargs)`
    Creates an icon using lucide icons

- `def UkIconLink(icon, height, width, stroke_width, cls, button, **kwargs)`
    Creates an icon link using lucide icons

- `def DiceBearAvatar(seed_name, h, w)`
    Creates an Avatar using https://dicebear.com/

- `def Center(*c, **kwargs)`
    Centers contents both vertically and horizontally by default

- `class FlexT(Enum)`
    Flexbox modifiers using Tailwind CSS
    Members: block, inline, left, center, right, between, around, stretch, top, middle, bottom, row, row_reverse, column, column_reverse, nowrap, wrap, wrap_reverse


- `def Grid(*div, **kwargs)`
    Creates a responsive grid layout with smart defaults based on content

- `def DivFullySpaced(*c, **kwargs)`
    Creates a flex div with it's components having as much space between them as possible

- `def DivCentered(*c, **kwargs)`
    Creates a flex div with it's components centered in it

- `def DivLAligned(*c, **kwargs)`
    Creates a flex div with it's components aligned to the left

- `def DivRAligned(*c, **kwargs)`
    Creates a flex div with it's components aligned to the right

- `def DivVStacked(*c, **kwargs)`
    Creates a flex div with it's components stacked vertically

- `def DivHStacked(*c, **kwargs)`
    Creates a flex div with it's components stacked horizontally

- `class NavT(Enum)`
    Members: default, primary, secondary


- `def NavContainer(*li, **kwargs)`
    Creates a navigation container (useful for creating a sidebar navigation).  A Nav is a list (NavBar is something different)

- `def NavParentLi(*nav_container, **kwargs)`
    Creates a navigation list item with a parent nav for nesting

- `def NavDividerLi(*c, **kwargs)`
    Creates a navigation list item with a divider

- `def NavHeaderLi(*c, **kwargs)`
    Creates a navigation list item with a header

- `def NavSubtitle(*c, **kwargs)`
    Creates a navigation subtitle

- `def NavCloseLi(*c, **kwargs)`
    Creates a navigation list item with a close button

- `class ScrollspyT(Enum)`
    Members: underline, bold

- `def NavBar(*c)`
    Creates a responsive navigation bar with mobile menu support

- `def SliderContainer(*c, **kwargs)`
    Creates a slider container

- `def SliderItems(*c, **kwargs)`
    Creates a slider items container

- `def SliderNav(cls, prev_cls, next_cls, **kwargs)`
    Navigation arrows for Slider component

- `def Slider(*c, **kwargs)`
    Creates a slider with optional navigation arrows

- `def DropDownNavContainer(*li, **kwargs)`
    A Nav that is part of a DropDown

- `def TabContainer(*li, **kwargs)`
    A TabContainer where children will be different tabs

- `class CardT(Enum)`
    Card styles from UIkit
    Members: default, primary, secondary, destructive, hover

- `def CardTitle(*c, **kwargs)`
    Creates a card title

- `def CardHeader(*c, **kwargs)`
    Creates a card header

- `def CardBody(*c, **kwargs)`
    Creates a card body

- `def CardFooter(*c, **kwargs)`
    Creates a card footer

- `def CardContainer(*c, **kwargs)`
    Creates a card container

- `def Card(*c, **kwargs)`
    Creates a Card with a header, body, and footer

- `class TableT(Enum)`
    Members: divider, striped, hover, sm, lg, justify, middle, responsive

- `def Table(*c, **kwargs)`
    Creates a table

- `def TableFromLists(header_data, body_data, footer_data, header_cell_render, body_cell_render, footer_cell_render, cls, sortable, **kwargs)`
    Creates a Table from a list of header data and a list of lists of body data

- `def TableFromDicts(header_data, body_data, footer_data, header_cell_render, body_cell_render, footer_cell_render, cls, sortable, **kwargs)`
    Creates a Table from a list of header data and a list of dicts of body data

- `def apply_classes(html_str, class_map, class_map_mods)`
    Apply classes to html string

- `def render_md(md_content, class_map, class_map_mods)`
    Renders markdown using mistletoe and lxml