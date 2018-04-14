package ee.microdeduplication.processWarcFiles.utils;

import junit.framework.TestCase;

public class MicroDataExtractionTest extends TestCase {

    // Todo test doctype fix aswell

    public void testInsertDoctypeFromSourceSimple()
    {
        String toBeReplaced = "<html that should stay>";
        String noHtmlTag = "<?xml version=\"1.0\" standalone=\"yes\"?><body>";

        String test1 = "<!DOCTYPE html>";
        String test2 = "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\" \"http://www.w3.org/TR/html4/loose.dtd\">";

        String result;

        result = MicroDataExtraction.insertDoctypeFromSource(toBeReplaced, test1);
        assertEquals(result, test1 + "\n" + toBeReplaced);

        result = MicroDataExtraction.insertDoctypeFromSource(toBeReplaced, test2);
        assertEquals(result, test2 + "\n" + toBeReplaced);

        result = MicroDataExtraction.insertDoctypeFromSource(noHtmlTag, test2);
        assertEquals(result, "<?xml version=\"1.0\" standalone=\"yes\"?>" + "\n" + test2 + "<body>");
    }


    public void testInsertDoctypeFromSourceNoDoctype()
    {
        String toBeReplaced = "<html tag>";

        String test1 = "<DOCTYPE html>";
        String test2 =  "<html></html>";

        String result;

        result = MicroDataExtraction.insertDoctypeFromSource(toBeReplaced, test1);
        assertEquals(result, toBeReplaced);

        result = MicroDataExtraction.insertDoctypeFromSource(toBeReplaced, test2);
        assertEquals(result, toBeReplaced);
    }


    public void testInsertDoctypeFromSourceRealWorldExample()
    {
        String toBeReplaced = "<?xml version=\"1.0\" standalone=\"yes\"?>\n" +
                "\n" +
                "\n" +
                "\n" +
                "<html lang=\"en\" prefix=\"dcam: http://purl.org/dc/dcam/ dctype: http://purl.org/dc/dcmitype/\" resource=\"http://purl.org/dc/\" xmlns=\"http://www.w3.org/1999/xhtml\">\n" +
                "<head>\n" +
                "<title>DCMI Metadata Terms</title>\n" +
                "\n" +
                "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\"></meta>\n" +
                "<link rel=\"stylesheet\" href=\"/css/default.css\" type=\"text/css\"></link>\n" +
                "<link rel=\"dcterms:tableOfContents\" href=\"#contents\" title=\"Table of Contents\"></link>\n" +
                "<style type=\"text/css\">\n" +
                "          \n" +
                "          \n" +
                "                                tr.attribute th {\n" +
                "                                        background-color: #fff;\n" +
                "                                }\n" +
                "                                table.legend,\n" +
                "                                table.references {\n" +
                "                                        margin-left: 2.5%;\n" +
                "                                        margin-right: 2.5%;\n" +
                "                                        width: 95%;\n" +
                "                                        table-layout:fixed;\n" +
                "                                        border-width: 0;\n" +
                "                                        }\n" +
                "                                .legend th.label,\n" +
                "                                .references th.abbrev {\n" +
                "                                        width: 20%;\n" +
                "                                }\n" +
                "                                .legend td.definition,\n" +
                "                                .references td.citation {\n" +
                "                                        width: 80%;\n" +
                "                                }\n" +
                "                                abbr.xmlns {\n" +
                "                                        font-style: italic;\n" +
                "                                        border-bottom: 0;\n" +
                "                                }\n" +
                "                                table.border.index td,\n" +
                "                                table.border.index th {\n" +
                "                                        border-spacing: 0;\n" +
                "                                }\n" +
                "                                table.index th {\n" +
                "                                        border-right: 1px solid #CCC; border-bottom: 1px solid #CCC; padding: .5em; /* FIXME: if this selector gets added to table.border th, then these duplicate CSS settings won't be needed. The ones below are the overrides */\n" +
                "                                        font-weight: normal;\n" +
                "                                        background-color: transparent;\n" +
                "                                }\n" +
                "                                \n" +
                "</style>\n" +
                "</head>\n" +
                "<body></body></html>";

        String source = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n" +
                "\n" +
                "<!DOCTYPE html>\n" +
                "<html prefix=\"dcam: http://purl.org/dc/dcam/ dctype: http://purl.org/dc/dcmitype/\" lang=\"en\" resource=\"http://purl.org/dc/\" xmlns:xhtml=\"http://www.w3.org/1999/xhtml\" xmlns=\"http://www.w3.org/1999/xhtml\">\n" +
                "<head>\n" +
                "<title>DCMI Metadata Terms</title>\n" +
                "\n" +
                "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\"/>\n" +
                "<link rel=\"stylesheet\" href=\"/css/default.css\" type=\"text/css\"/>\n" +
                "<link rel=\"dcterms:tableOfContents\" href=\"#contents\" title=\"Table of Contents\"/>\n" +
                "<style type=\"text/css\">\n" +
                "          \n" +
                "          \n" +
                "                                tr.attribute th {\n" +
                "                                        background-color: #fff;\n" +
                "                                }\n" +
                "                                table.legend,\n" +
                "                                table.references {\n" +
                "                                        margin-left: 2.5%;\n" +
                "                                        margin-right: 2.5%;\n" +
                "                                        width: 95%;\n" +
                "                                        table-layout:fixed;\n" +
                "                                        border-width: 0;\n" +
                "                                        }\n" +
                "                                .legend th.label,\n" +
                "                                .references th.abbrev {\n" +
                "                                        width: 20%;\n" +
                "                                }\n" +
                "                                .legend td.definition,\n" +
                "                                .references td.citation {\n" +
                "                                        width: 80%;\n" +
                "                                }\n" +
                "                                abbr.xmlns {\n" +
                "                                        font-style: italic;\n" +
                "                                        border-bottom: 0;\n" +
                "                                }\n" +
                "                                table.border.index td,\n" +
                "                                table.border.index th {\n" +
                "                                        border-spacing: 0;\n" +
                "                                }\n" +
                "                                table.index th {\n" +
                "                                        border-right: 1px solid #CCC; border-bottom: 1px solid #CCC; padding: .5em; /* FIXME: if this selector gets added to table.border th, then these duplicate CSS settings won't be needed. The ones below are the overrides */\n" +
                "                                        font-weight: normal;\n" +
                "                                        background-color: transparent;\n" +
                "                                }\n" +
                "                                \n" +
                "</style>\n" +
                "</head>\n" +
                "<body></body></html>";

        String result;
        String expectedResult = "<?xml version=\"1.0\" standalone=\"yes\"?>\n" +
                "\n" +
                "\n" +
                "\n" +
                "<!DOCTYPE html>\n" +
                "<html lang=\"en\" prefix=\"dcam: http://purl.org/dc/dcam/ dctype: http://purl.org/dc/dcmitype/\" resource=\"http://purl.org/dc/\" xmlns=\"http://www.w3.org/1999/xhtml\">\n" +
                "<head>\n" +
                "<title>DCMI Metadata Terms</title>\n" +
                "\n" +
                "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\"></meta>\n" +
                "<link rel=\"stylesheet\" href=\"/css/default.css\" type=\"text/css\"></link>\n" +
                "<link rel=\"dcterms:tableOfContents\" href=\"#contents\" title=\"Table of Contents\"></link>\n" +
                "<style type=\"text/css\">\n" +
                "          \n" +
                "          \n" +
                "                                tr.attribute th {\n" +
                "                                        background-color: #fff;\n" +
                "                                }\n" +
                "                                table.legend,\n" +
                "                                table.references {\n" +
                "                                        margin-left: 2.5%;\n" +
                "                                        margin-right: 2.5%;\n" +
                "                                        width: 95%;\n" +
                "                                        table-layout:fixed;\n" +
                "                                        border-width: 0;\n" +
                "                                        }\n" +
                "                                .legend th.label,\n" +
                "                                .references th.abbrev {\n" +
                "                                        width: 20%;\n" +
                "                                }\n" +
                "                                .legend td.definition,\n" +
                "                                .references td.citation {\n" +
                "                                        width: 80%;\n" +
                "                                }\n" +
                "                                abbr.xmlns {\n" +
                "                                        font-style: italic;\n" +
                "                                        border-bottom: 0;\n" +
                "                                }\n" +
                "                                table.border.index td,\n" +
                "                                table.border.index th {\n" +
                "                                        border-spacing: 0;\n" +
                "                                }\n" +
                "                                table.index th {\n" +
                "                                        border-right: 1px solid #CCC; border-bottom: 1px solid #CCC; padding: .5em; /* FIXME: if this selector gets added to table.border th, then these duplicate CSS settings won't be needed. The ones below are the overrides */\n" +
                "                                        font-weight: normal;\n" +
                "                                        background-color: transparent;\n" +
                "                                }\n" +
                "                                \n" +
                "</style>\n" +
                "</head>\n" +
                "<body></body></html>";

        result = MicroDataExtraction.insertDoctypeFromSource(toBeReplaced, source);
        assertEquals(result, expectedResult);
    }
}
