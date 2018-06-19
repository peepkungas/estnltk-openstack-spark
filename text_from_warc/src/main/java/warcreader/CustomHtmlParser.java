package warcreader;

import de.l3s.boilerpipe.BoilerpipeExtractor;
import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.document.TextDocument;
import de.l3s.boilerpipe.extractors.CommonExtractors;
import de.l3s.boilerpipe.sax.BoilerpipeSAXInput;
import org.apache.tika.config.ServiceLoader;
import org.apache.tika.detect.AutoDetectReader;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.CloseShieldInputStream;
import org.apache.tika.io.IOUtils;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.html.DefaultHtmlMapper;
//import org.apache.tika.parser.html.HtmlHandler;
import org.apache.tika.parser.html.HtmlMapper;
//import org.apache.tika.parser.html.XHTMLDowngradeHandler;
import org.apache.tika.sax.ContentHandlerDecorator;
import org.apache.tika.sax.TextContentHandler;
import org.apache.tika.sax.XHTMLContentHandler;
import org.ccil.cowan.tagsoup.HTMLSchema;
import org.ccil.cowan.tagsoup.Parser;
import org.ccil.cowan.tagsoup.Schema;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.python.core.PyException;
import org.python.core.PyInteger;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
// Modified by Madis-Karli Koppel
// Parsers HTML and extracts only text: discards html tags, and if possible then also head and footer tags

public class CustomHtmlParser extends AbstractParser {
    private static final long serialVersionUID = 7895315240498733128L;
    private static final Set<MediaType> SUPPORTED_TYPES = Collections.unmodifiableSet(new HashSet(Arrays.asList(MediaType.text("html"), MediaType.application("xhtml+xml"), MediaType.application("vnd.wap.xhtml+xml"), MediaType.application("x-asp"))));
    private static final ServiceLoader LOADER = new ServiceLoader(CustomHtmlParser.class.getClassLoader());
    private static final Schema HTML_SCHEMA = new HTMLSchema();
    private static boolean useBoilerPipe = false;
    private static boolean useJsoup = false;
    private static boolean useDragnet = true;

    public CustomHtmlParser() {
    }

    public Set<MediaType> getSupportedTypes(ParseContext context) {
        return SUPPORTED_TYPES;
    }

    public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context) throws IOException, SAXException, TikaException {

        // Use your own custom parser here
        // kind of hackish but does the job

        String contents = IOUtils.toString(stream);
        String textContent = "";


        if (useJsoup) {
            Document doc = Jsoup.parse(contents);
            textContent = doc.body().text();
        }

        if (useBoilerPipe) {
            try {
                TextDocument textDocument = new BoilerpipeSAXInput(new InputSource(new StringReader(contents))).getTextDocument();
                textContent = CommonExtractors.ARTICLE_EXTRACTOR.getText(textDocument);
                System.out.println(textContent);

            } catch (BoilerpipeProcessingException e) {
                e.printStackTrace();
            }
        }

        if(contents.length() == 0)
            useDragnet = false;

        if (useDragnet) {
//            try {
//                PythonInterpreter python = new PythonInterpreter();
//                python.set("number1", new PyInteger(123));
//                python.set("number2", new PyInteger(11));
//                python.exec("number3 = number1+number2");
//                PyObject number3 = python.get("number3");
//                System.out.println("val : " + number3.toString());
//            } catch (PyException e){
//                e.printStackTrace();
//            }
            try {
                String prg = "# coding=UTF-8\n" +
                        "from dragnet import extract_content\n" +
                        "content=\"\"\""+ contents +"\"\"\"\n" +
                        "extracted = extract_content(content)\n" +
                        "print unicode(extracted).encode('utf-8')\n";

                BufferedWriter out = new BufferedWriter(new FileWriter("test1.py"));
                out.write(prg);
                out.close();

                ProcessBuilder pb = new ProcessBuilder("python", "test1.py");
                Process p = pb.start();

                // Wait for Python to end
//                p.waitFor();

                BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
                BufferedReader err = new BufferedReader(new InputStreamReader(p.getErrorStream()));

                String output = IOUtils.toString(in);
                String error = IOUtils.toString(err);

                System.out.println("_1_ " + output);
                System.out.println("_2_ " + error);

                in.close();

            } catch (Exception e) {
                System.out.println(e);
            }
        }

    InputStream s = IOUtils.toInputStream(textContent);

    AutoDetectReader reader = new AutoDetectReader(new CloseShieldInputStream(s), metadata, LOADER);

        try

    {
        Charset charset = reader.getCharset();
        String previous = metadata.get("Content-Type");
        if (previous == null || previous.startsWith("text/html")) {
            MediaType type = new MediaType(MediaType.TEXT_HTML, charset);
            metadata.set("Content-Type", type.toString());
        }

        metadata.set("Content-Encoding", charset.name());
        HtmlMapper mapper = context.get(HtmlMapper.class, new HtmlParserMapper());
        Parser parser = new Parser();
        parser.setProperty("http://www.ccil.org/~cowan/tagsoup/properties/schema", HTML_SCHEMA);
        parser.setFeature("http://www.ccil.org/~cowan/tagsoup/features/ignore-bogons", true);
        parser.setContentHandler(new XHTMLDowngradeHandler(new HtmlHandler(mapper, handler, metadata)));

        // TODO find a way to not call parser and just set the expected output
        parser.parse(reader.asInputSource());
    } finally

    {
        reader.close();
    }
}

/**
 * @deprecated
 */
private class HtmlParserMapper implements HtmlMapper {
    private HtmlParserMapper() {
    }

    public String mapSafeElement(String name) {
        return DefaultHtmlMapper.INSTANCE.mapSafeElement(name);
    }

    public boolean isDiscardElement(String name) {
        return DefaultHtmlMapper.INSTANCE.isDiscardElement(name);
    }

    public String mapSafeAttribute(String elementName, String attributeName) {
        return DefaultHtmlMapper.INSTANCE.mapSafeAttribute(elementName, attributeName);
    }
}
}

// Nothing is changed below
class XHTMLDowngradeHandler extends ContentHandlerDecorator {
    public XHTMLDowngradeHandler(ContentHandler handler) {
        super(handler);
    }

    public void startElement(String uri, String localName, String name, Attributes atts) throws SAXException {
        String upper = localName.toUpperCase(Locale.ENGLISH);
        AttributesImpl attributes = new AttributesImpl();

        for (int i = 0; i < atts.getLength(); ++i) {
            String auri = atts.getURI(i);
            String local = atts.getLocalName(i);
            String qname = atts.getQName(i);
            if ("".equals(auri) && !local.equals("xmlns") && !qname.startsWith("xmlns:")) {
                attributes.addAttribute(auri, local, qname, atts.getType(i), atts.getValue(i));
            }
        }

        super.startElement("", upper, upper, attributes);
    }

    public void endElement(String uri, String localName, String name) throws SAXException {
        String upper = localName.toUpperCase(Locale.ENGLISH);
        super.endElement("", upper, upper);
    }

    public void startPrefixMapping(String prefix, String uri) {
    }

    public void endPrefixMapping(String prefix) {
    }
}


class HtmlHandler extends TextContentHandler {
    private static final Set<String> URI_ATTRIBUTES = new HashSet(Arrays.asList("src", "href", "longdesc", "cite"));
    private final HtmlMapper mapper;
    private final XHTMLContentHandler xhtml;
    private final Metadata metadata;
    private int bodyLevel;
    private int discardLevel;
    private int titleLevel;
    private final StringBuilder title;
    private static final Pattern ICBM = Pattern.compile("\\s*(-?\\d+\\.\\d+)[,\\s]+(-?\\d+\\.\\d+)\\s*");

    private HtmlHandler(HtmlMapper mapper, XHTMLContentHandler xhtml, Metadata metadata) {
        super(xhtml);
        this.bodyLevel = 0;
        this.discardLevel = 0;
        this.titleLevel = 0;
        this.title = new StringBuilder();
        this.mapper = mapper;
        this.xhtml = xhtml;
        this.metadata = metadata;
        if (metadata.get("Content-Location") == null) {
            String name = metadata.get("resourceName");
            if (name != null) {
                name = name.trim();

                try {
                    new URL(name);
                    metadata.set("Content-Location", name);
                } catch (MalformedURLException var6) {
                    ;
                }
            }
        }

    }

    public HtmlHandler(HtmlMapper mapper, ContentHandler handler, Metadata metadata) {
        this(mapper, new XHTMLContentHandler(handler, metadata), metadata);
    }

    public void startElement(String uri, String local, String name, Attributes atts) throws SAXException {
        if ("TITLE".equals(name) || this.titleLevel > 0) {
            ++this.titleLevel;
        }

        if ("BODY".equals(name) || "FRAMESET".equals(name) || this.bodyLevel > 0) {
            ++this.bodyLevel;
        }

        if (this.mapper.isDiscardElement(name) || this.discardLevel > 0) {
            ++this.discardLevel;
        }

        if (this.bodyLevel == 0 && this.discardLevel == 0) {
            if ("META".equals(name) && atts.getValue("content") != null) {
                if (atts.getValue("http-equiv") != null) {
                    this.addHtmlMetadata(atts.getValue("http-equiv"), atts.getValue("content"));
                } else if (atts.getValue("name") != null) {
                    this.addHtmlMetadata(atts.getValue("name"), atts.getValue("content"));
                }
            } else if ("BASE".equals(name) && atts.getValue("href") != null) {
                this.startElementWithSafeAttributes("base", atts);
                this.xhtml.endElement("base");
                this.metadata.set("Content-Location", this.resolve(atts.getValue("href")));
            } else if ("LINK".equals(name)) {
                this.startElementWithSafeAttributes("link", atts);
                this.xhtml.endElement("link");
            }
        }

        if (this.bodyLevel > 0 && this.discardLevel == 0) {
            String safe = this.mapper.mapSafeElement(name);
            if (safe != null) {
                this.startElementWithSafeAttributes(safe, atts);
            }
        }

        this.title.setLength(0);
    }

    private void addHtmlMetadata(String name, String value) {
        if (name != null && value != null) {
            if (name.equalsIgnoreCase("ICBM")) {
                Matcher m = ICBM.matcher(value);
                if (m.matches()) {
                    this.metadata.set("ICBM", m.group(1) + ", " + m.group(2));
                    this.metadata.set(Metadata.LATITUDE, m.group(1));
                    this.metadata.set(Metadata.LONGITUDE, m.group(2));
                } else {
                    this.metadata.set("ICBM", value);
                }
            } else if (name.equalsIgnoreCase("Content-Type")) {
                MediaType type = MediaType.parse(value);
                if (type != null) {
                    this.metadata.set("Content-Type", type.toString());
                } else {
                    this.metadata.set("Content-Type", value);
                }
            } else {
                this.metadata.set(name, value);
            }
        }

    }

    private void startElementWithSafeAttributes(String name, Attributes atts) throws SAXException {
        if (atts.getLength() == 0) {
            this.xhtml.startElement(name);
        } else {
            boolean isObject = name.equals("object");
            String codebase = null;
            if (isObject) {
                codebase = atts.getValue("", "codebase");
                if (codebase != null) {
                    codebase = this.resolve(codebase);
                } else {
                    codebase = this.metadata.get("Content-Location");
                }
            }

            AttributesImpl newAttributes = new AttributesImpl(atts);

            for (int att = 0; att < newAttributes.getLength(); ++att) {
                String attrName = newAttributes.getLocalName(att);
                String normAttrName = this.mapper.mapSafeAttribute(name, attrName);
                if (normAttrName == null) {
                    newAttributes.removeAttribute(att);
                    --att;
                } else {
                    newAttributes.setLocalName(att, normAttrName);
                    if (URI_ATTRIBUTES.contains(normAttrName)) {
                        newAttributes.setValue(att, this.resolve(newAttributes.getValue(att)));
                    } else if (isObject && "codebase".equals(normAttrName)) {
                        newAttributes.setValue(att, codebase);
                    } else if (isObject && ("data".equals(normAttrName) || "classid".equals(normAttrName))) {
                        newAttributes.setValue(att, this.resolve(codebase, newAttributes.getValue(att)));
                    }
                }
            }

            if ("img".equals(name) && newAttributes.getValue("", "alt") == null) {
                newAttributes.addAttribute("", "alt", "alt", "CDATA", "");
            }

            this.xhtml.startElement(name, newAttributes);
        }
    }

    public void endElement(String uri, String local, String name) throws SAXException {
        if (this.bodyLevel > 0 && this.discardLevel == 0) {
            String safe = this.mapper.mapSafeElement(name);
            if (safe != null) {
                this.xhtml.endElement(safe);
            } else if (XHTMLContentHandler.ENDLINE.contains(name.toLowerCase(Locale.ENGLISH))) {
                this.xhtml.newline();
            }
        }

        if (this.titleLevel > 0) {
            --this.titleLevel;
            if (this.titleLevel == 0) {
                this.metadata.set(TikaCoreProperties.TITLE, this.title.toString().trim());
            }
        }

        if (this.bodyLevel > 0) {
            --this.bodyLevel;
        }

        if (this.discardLevel > 0) {
            --this.discardLevel;
        }

    }

    public void characters(char[] ch, int start, int length) throws SAXException {
        if (this.titleLevel > 0 && this.bodyLevel == 0) {
            this.title.append(ch, start, length);
        }

        if (this.bodyLevel > 0 && this.discardLevel == 0) {
            super.characters(ch, start, length);
        }

    }

    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        if (this.bodyLevel > 0 && this.discardLevel == 0) {
            super.ignorableWhitespace(ch, start, length);
        }

    }

    private String resolve(String url) {
        return this.resolve(this.metadata.get("Content-Location"), url);
    }

    private String resolve(String base, String url) {
        url = url.trim();
        String lower = url.toLowerCase(Locale.ENGLISH);
        if (base != null && !lower.startsWith("urn:") && !lower.startsWith("mailto:") && !lower.startsWith("tel:") && !lower.startsWith("data:") && !lower.startsWith("javascript:") && !lower.startsWith("about:")) {
            try {
                URL baseURL = new URL(base.trim());
                String path = baseURL.getPath();
                return url.startsWith("?") && path.length() > 0 && !path.endsWith("/") ? (new URL(baseURL.getProtocol(), baseURL.getHost(), baseURL.getPort(), baseURL.getPath() + url)).toExternalForm() : (new URL(baseURL, url)).toExternalForm();
            } catch (MalformedURLException var6) {
                return url;
            }
        } else {
            return url;
        }
    }
}



