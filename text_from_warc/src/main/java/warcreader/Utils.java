package warcreader;

import org.jwat.warc.WarcRecord;

import java.net.MalformedURLException;
import java.net.URL;

public class Utils {

    // consider adding json here
    // TODO investigate what is application/x-wof
    public static String[] ignoreContentTypes = new String[]{
            "image/", "images/", "img/", "image\\", "imageformat:", "image%2Fpng", "video/", "audio/",
            "x-font/", "font/", "text/css", "text/javascript", "javascript", "download",
            "application/x-gzip", "application/x-shockwave-flash", "application/vnd.ms-fontobject", "application/json",
            "application/x-x509-ca-cert", "application/x-javascript", "application/javascript", "application/vnd.javascript",
            "application/font-woff",
            "application/x-font", "application/image", "application/zip", "binary/octet-stream", "text/x-component"
    };

    // what else to check
    // less than 4 000 occurences
    // application/x-www-form-urlencoded
    // application/vnd.ms-cab-compressed
    // less than 3 000 occurences
    // text/turtle
    // less than 2 000 occurences
    // application/thumb
    // less than 1 000 occurences
    // application/unknown
    // text/x-vcard
    // application/vnd.google-earth.kml+xml
    // application/x-wais-source
    // less than 500 occurences
    // application/vnd.apple.mpegurl
    // application/save-as
    // application/binary
    // $mime
    // application/font-sfnt
    // application/vnc.ms-fontobject
    // application/save application/x-zip
    // application/x-rar-compressed
    // application/zip
    // application/vnd.etsi.asic-e+zip
    // application/ogg
    // application/epub+zip
    // application/x-mpegurl
    // httpd/unix-directory
    // application/x-msdos-program
    // application/x-mobipocket-ebook
    // application/java-archive
    // application/vnd.google-earth.kmz
    // less than 100
    // application/gzip

    public static boolean inList(String inputStr, String[] items) {
        inputStr = inputStr.toLowerCase();

        // TODO hardcoded
        // these are special cases when we are checking content types
        // all of them were encountered in the wild
        if (inputStr.equals("jpg"))
            return true;
        if (inputStr.equals("jpeg"))
            return true;
        if (inputStr.equals(".jpg"))
            return true;
        if (inputStr.equals("png"))
            return true;
        if (inputStr.equals(".png"))
            return true;
        // image%2Fpng was not somewhy matched in the general content-type filter
        if (inputStr.endsWith("png") && inputStr.startsWith("image"))
            return true;

        for (int i = 0; i < items.length; i++) {
            if (inputStr.contains(items[i])) {
                return true;
            }
            if (items[i].contains(inputStr)) {
                return true;
            }
        }
        return false;
    }

    public static TupleObject generateID(WarcRecord warcRecord){
        // Construct the ID as it was in nutch, example:
        // http::g.delfi.ee::/s/img/back_grey.gif::null::20150214090921

        TupleObject out = new TupleObject();

        try {
            String date = warcRecord.getHeader("WARC-Date").value;
            date = date.replaceAll("-|T|Z|:", "");
            URL url = new URL(warcRecord.getHeader("WARC-Target-URI").value);

            String protocol = url.getProtocol();
            String hostname = url.getHost();
            String urlpath = url.getPath();
            String param = url.getQuery();

            out.id = protocol + "::" + hostname + "::" + urlpath + "::" + param + "::" + date;
            out.urlpath = urlpath;

            return out;
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (NullPointerException e){

        }

        return out;
    }
}
