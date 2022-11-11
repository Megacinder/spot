import pandas as pd


xml = """<?xml version='1.0' encoding='utf-8'?>
 <response>
  <row>
    <station id="40850" name="Library"/>
    <month>2020-09-01T00:00:00</month>
    <rides>
      <avg_weekday_rides>864.2</avg_weekday_rides>
      <avg_saturday_rides>534</avg_saturday_rides>
      <avg_sunday_holiday_rides>417.2</avg_sunday_holiday_rides>
    </rides>
  </row>
  <row>
    <station id="41700" name="Washington/Wabash"/>
    <month>2020-09-01T00:00:00</month>
    <rides>
      <avg_weekday_rides>2707.4</avg_weekday_rides>
      <avg_saturday_rides>1909.8</avg_saturday_rides>
      <avg_sunday_holiday_rides>1438.6</avg_sunday_holiday_rides>
    </rides>
  </row>
  <row>
    <station id="40380" name="Clark/Lake"/>
    <month>2020-09-01T00:00:00</month>
    <rides>
      <avg_weekday_rides>2949.6</avg_weekday_rides>
      <avg_saturday_rides>1657</avg_saturday_rides>
      <avg_sunday_holiday_rides>1453.8</avg_sunday_holiday_rides>
    </rides>
  </row>
 </response>"""


xsl = """<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
   <xsl:output method="xml" omit-xml-declaration="no" indent="yes"/>
   <xsl:strip-space elements="*"/>
   <xsl:template match="/response">
      <xsl:copy>
        <xsl:apply-templates select="row"/>
      </xsl:copy>
   </xsl:template>
   <xsl:template match="row">
      <xsl:copy>
        <station_id><xsl:value-of select="station/@id"/></station_id>
        <station_name><xsl:value-of select="station/@name"/></station_name>
        <xsl:copy-of select="month|rides/*"/>
      </xsl:copy>
   </xsl:template>
 </xsl:stylesheet>"""


output = """<?xml version='1.0' encoding='utf-8'?>
 <response>
   <row>
      <station_id>40850</station_id>
      <station_name>Library</station_name>
      <month>2020-09-01T00:00:00</month>
      <avg_weekday_rides>864.2</avg_weekday_rides>
      <avg_saturday_rides>534</avg_saturday_rides>
      <avg_sunday_holiday_rides>417.2</avg_sunday_holiday_rides>
   </row>
   <row>
      <station_id>41700</station_id>
      <station_name>Washington/Wabash</station_name>
      <month>2020-09-01T00:00:00</month>
      <avg_weekday_rides>2707.4</avg_weekday_rides>
      <avg_saturday_rides>1909.8</avg_saturday_rides>
      <avg_sunday_holiday_rides>1438.6</avg_sunday_holiday_rides>
   </row>
   <row>
      <station_id>40380</station_id>
      <station_name>Clark/Lake</station_name>
      <month>2020-09-01T00:00:00</month>
      <avg_weekday_rides>2949.6</avg_weekday_rides>
      <avg_saturday_rides>1657</avg_saturday_rides>
      <avg_sunday_holiday_rides>1453.8</avg_sunday_holiday_rides>
   </row>
 </response>"""


df = pd.read_xml(xml, stylesheet=xsl)
breakpoint()