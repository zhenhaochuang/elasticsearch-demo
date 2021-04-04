package com.chuang.utils;

import com.chuang.pojo.Content;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

@Component
public class HtmlParseUtil {
    public static void main(String[] args) throws IOException {
        new HtmlParseUtil().parseJD("vue").forEach(System.out::println);
    }
    public List<Content> parseJD(String keywords) throws IOException {
        String url = "https://search.jd.com/Search?keyword="+keywords;

        Document document = Jsoup.parse(new URL(url),3000);
        Element element = document.getElementById("J_goodsList");
        Elements elements = element.getElementsByTag("li");

        ArrayList<Content> goodsList = new ArrayList<>();
        for (Element el : elements){
            String img = el.getElementsByTag("img").eq(0).attr("data-lazy-img");
            String price = el.getElementsByClass("p-price").eq(0).text();
            String title = el.getElementsByClass("p-name").eq(0).text();

            Content content = new Content();
            content.setTitle(title);
            content.setPrice(price);
            content.setImg(img);
            goodsList.add(content);
        }
        return goodsList;
    }
}
