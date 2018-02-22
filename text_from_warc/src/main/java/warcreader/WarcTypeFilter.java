package warcreader;

import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.jwat.warc.WarcRecord;
import scala.Tuple2;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

/**
 * Created by Madis-Karli Koppel on 13/12/2017.
 */
public class WarcTypeFilter implements Function<Tuple2<LongWritable, WarcRecord>, Boolean>, Serializable {

    private static final Logger logger = LogManager.getLogger(TextExtractorPairFunction.class);

    // old filters that were used before, for octavios text
//    private static final String[] urlList = "http://lexus.ee,http://lexusteenindus.ee,http://toyotalisavarustus.ee,https://toyota.ee,www.silberauto.ee,https://auto100.ee,http://amservauto.ee,https://kia.ee,http://mollereesti.ee,http://www.moller.ee,http://www.mollerauto.ee,http://www.peugeot.ee,http://unitedmotors.ee,http://veho.ee,http://bumerange.ee,http://fordtrucks.ee,http://infoauto.ee,http://volvo.ee,http://www.iauto.ee,https://ford.ee,http://bmw-motorrad.ee,http://esmaauto.ee,http://inchcape.ee,http://jaguar.ee,http://landrover.ee,http://mazda.ee,http://mini.ee,http://vilojett.ee,http://volvoteenindus.ee,https://bmw.ee,http://audi.ee,http://elkeauto.ee,http://volvo.ee,http://abcmotors.ee,http://audihooldus.ee,http://skoda.ee,http://mariineauto.ee,http://citymotors.ee,http://dacia.ee,http://dfsk.ee,http://corvette.ee,http://saab.ee,https://vikingmotors.ee,http://www.mototemyth.eu,http://catwees.ee,http://ascar.ee,http://chevy.ee,http://autospirit.ee,http://infoauto.ee,http://www.hotels.tallink.com,http://amigo.ee,http://viru.ee,https://www.radissonblu.com,http://cafemademoiselle.ee,http://meritonsport.ee,http://meresuu.ee,http://spatallinn.ee,http://wow.ee,https://viimsikino.ee,http://cafeswiss.ee,http://horisontrestoran.ee,http://no3.ee,http://swissoteldining.ee,http://www.tallinn.swissotel.com,http://www.radissonblu.com,http://estoniaspa.ee,http://spaestonia.ee,http://nordichotels.ee,http://restaurantmonaco.ee,http://www.nordichotels.eu,http://spatervis.ee,http://aqvahotels.ee,http://aqvaspordikeskus.ee,http://spa.ee,http://spasport.ee,http://terviseparadiis.ee,http://mekk.ee,https://tallinnhotels.ee,http://www.kalevspa.ee,https://www.parkinn.com,www.laulasmaa.ee,http://www.liivarand.ee,https://www.telegraafhotel.com,http://viiking.ee,http://dorpat.ee,http://www.oc.eu/,www.euroopa.ee,https://strand.ee,www.gospa.ee,http://playtech.ee,www.zeroturnaround.com,https://helmes.ee,https://nortal.ee,https://www.nortal.com,https://tieto.ee,https://www.pipedrive.com,https://www.symantec.com,http://businessobjects.ee,https://proekspert.ee,https://www.cgi.com,http://datel.ee,http://piiriveekogu.ee,http://cyber.ee,https://ekta.ee,https://www.starship.xyz/,http://icefire.ee,http://www.videobet.ee,http://www.industry62.com,https://www.twilio.com,http://hireright.ee,http://derivco.ee,https://www.guardtime.com,http://codeborne.com,http://codeborne.ee,http://itpealinn.ee,http://netgroup.ee,https://relax-gaming.com,http://raviminfo.ee,https://mooncascade.com,http://axinom.ee,http://rkas.ee,www.rimi.ee,http://citycon.ee,http://roccaalmare.ee,http://ulemiste.ee,https://www.nginvest.ee,www.technopolis.ee,http://blrt.ee,http://www.bsr.ee,www.kristiine.com,http://jarvekeskus.ee,http://solaris.ee,http://olerex.ee,https://lõunakeskus.ee,http://lounakeskus.com,www.kaarsilla.ee,https://www.kapitel.ee,http://lasnamaecentrum.ee,www.tasku.ee,http://www.feenoks.ee,http://mainorulemiste.ee,http://ulemistecity.ee,http://opiku.ee,http://nh-cap.com"
    // New filters, only used with 2015 crawls
    private static final String[] urlList = "http://www.bestserv.ee,http://www.martens.ee,https://www.kvanta.org,http://veiniklubi.ee,http://wineagency.ee,http://www.veinimaailm.ee,http://eesti-veiniakadeemia.ee,http://laevaselts.ee,http://www.olympic-casino.com,http://okoehitus.ee,http://aspekt.ee,http://globalnet.ee,http://www.agenor.ee,http://revalcapital.ee,http://ecofor.ee,http://front-line.ee,http://citycentrum.ee,http://applaud.ee,http://veebipartner.ee,http://www.gispotec.com,http://www.cognac.ee,http://eestimesi.ee,http://nordcup.ee,http://www.samsaara.info"
            .replaceAll("https", "http")
            .replaceAll("http://", "")
            .split(",");

    public Boolean call(Tuple2<LongWritable, WarcRecord> s) {
        String header;
        String hostname;
        int size;

        /*
        Some WARC records do not have Content-Type header, such as
        WARC-Type : resource
        WARC-Target-URI : metadata://netarkivet.dk/crawl/setup/duplicatereductionjobs?majorversion=1&minorversion=0&harvestid=1&harvestnum=0&jobid=10
        WARC-Date : 2015-02-15T21:23:35Z
        WARC-Block-Digest : sha1:da39a3ee5e6b4b0d3255bfef95601890afd80709
        WARC-Warcinfo-ID : <urn:uuid:4916247c-8bfc-428d-b27d-4a28372dbf73>
        WARC-IP-Address : 127.0.1.1
        WARC-Record-ID : <urn:uuid:44b27d4f-06d8-46f9-9c18-441d173dd925>
        Content-Length : 0
        */

        try {
            header = s._2.getHeader("Content-Type").value;
            size = Integer.parseInt(s._2.getHeader("Content-Length").value);
            URL url = new URL(s._2.getHeader("WARC-Target-URI").value);
            hostname = url.getHost();
//            logger.error("uri " + url);
//            logger.error(header);
//            logger.error("size " + size);

        } catch (NullPointerException e) {
            return false;
        } catch (MalformedURLException e) {
            return false;
        } catch (NumberFormatException e){
            return false;
        }

        // Ignore WARC specific content and DNS files
        if (header.equals("application/warc-fields")) return false;

        if (header.equals("text/dns")) return false;

        if (s._2.getHeader("WARC-Target-URI").value.startsWith("metadata")) return false;

        // one file was 573650557 and it OutOfMemoryError
        // limit is set to 769 MB
        if (size > 100000000) return false;

        // TODO for readability move it to its own negative if
        // NEGATIVE IF!
//        if (!inList(hostname, urlList)) return false;

        return true;
    }

    public static boolean inList(String inputStr, String[] items) {
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

}
