-- PM Sample Data - Insert statements with LOB data
-- Run after pm_create.sql

-- Product information (standalone version)
INSERT INTO product_information VALUES
(1726, 'LCD Monitor 11/PM', 'LCD Monitor 11 inch with PM feature', 14, 1,
 INTERVAL '1' YEAR, 102092, 'orderable', 259, 208, 'http://example.com/lcd11');

INSERT INTO product_information VALUES
(2359, 'LCD Monitor 15/PM', 'LCD Monitor 15 inch with PM feature', 14, 2,
 INTERVAL '2' YEAR, 102092, 'orderable', 459, 368, 'http://example.com/lcd15');

INSERT INTO product_information VALUES
(3060, 'Monitor 17/HR', 'Monitor 17 inch high resolution', 14, 3,
 INTERVAL '3' YEAR, 102090, 'orderable', 299, 239, 'http://example.com/mon17');

INSERT INTO product_information VALUES
(3106, 'Monitor 19/SD', 'Monitor 19 inch standard definition', 14, 4,
 INTERVAL '2' YEAR, 102090, 'orderable', 399, 319, 'http://example.com/mon19');

COMMIT;

-- Print media with LOB data
-- Using EMPTY_BLOB() and EMPTY_CLOB() for initialization, then updating with actual data

-- First, insert rows with empty LOBs
INSERT INTO print_media (product_id, ad_id, ad_composite, ad_sourcetext, ad_finaltext, ad_fltextn, ad_photo, ad_header)
VALUES (
    1726,
    12001,
    EMPTY_BLOB(),
    'This is the source text for LCD Monitor 11/PM advertisement.
     Features include: PM display technology, 11-inch screen, low power consumption.
     Target audience: Small office/home office users.',
    'LCD Monitor 11/PM - Perfect for Your Desktop!
     Experience crystal clear visuals with our PM technology.
     11-inch screen | Energy efficient | 3-year warranty',
    N'LCD モニター 11/PM - デスクトップに最適！
      PM技術でクリスタルクリアなビジュアルを体験。
      11インチスクリーン | 省エネ | 3年保証',
    EMPTY_BLOB(),
    adheader_typ(
        'LCD11_PROMO_2024',
        DATE '2024-01-15',
        'Premium LCD Technology for the Modern Office',
        EMPTY_BLOB()
    )
);

INSERT INTO print_media (product_id, ad_id, ad_composite, ad_sourcetext, ad_finaltext, ad_fltextn, ad_photo, ad_header)
VALUES (
    2359,
    12002,
    EMPTY_BLOB(),
    'Source text for LCD Monitor 15/PM advertisement campaign.
     Key selling points: Larger 15-inch screen, enhanced PM technology,
     wider viewing angles. Ideal for professional use.',
    'LCD Monitor 15/PM - Professional Grade Display
     Larger screen, better visuals, superior PM technology.
     15-inch screen | Wide viewing angles | 2-year warranty',
    N'LCD モニター 15/PM - プロフェッショナルグレードディスプレイ
      大画面、優れたビジュアル、優れたPM技術。
      15インチスクリーン | 広い視野角 | 2年保証',
    EMPTY_BLOB(),
    adheader_typ(
        'LCD15_BUSINESS_2024',
        DATE '2024-02-20',
        'Business-Class Display Solution',
        EMPTY_BLOB()
    )
);

INSERT INTO print_media (product_id, ad_id, ad_composite, ad_sourcetext, ad_finaltext, ad_fltextn, ad_photo, ad_header)
VALUES (
    3060,
    12003,
    EMPTY_BLOB(),
    'High resolution 17-inch monitor advertisement.
     Technical specifications: 1920x1080 resolution, IPS panel,
     adjustable stand, VESA mount compatible.',
    'Monitor 17/HR - See Every Detail
     High resolution display for demanding users.
     17-inch IPS | Full HD | Adjustable stand | VESA ready',
    N'モニター 17/HR - すべてのディテールを見る
      要求の厳しいユーザー向けの高解像度ディスプレイ。
      17インチIPS | フルHD | 調整可能スタンド | VESA対応',
    EMPTY_BLOB(),
    adheader_typ(
        'MON17_DETAIL_2024',
        DATE '2024-03-10',
        'Precision Display for Creative Professionals',
        EMPTY_BLOB()
    )
);

COMMIT;

-- Insert nested table data for text documents
INSERT INTO TABLE(SELECT ad_textdocs_ntab FROM print_media WHERE ad_id = 12001)
VALUES (textdoc_typ('PDF', EMPTY_BLOB()));

INSERT INTO TABLE(SELECT ad_textdocs_ntab FROM print_media WHERE ad_id = 12001)
VALUES (textdoc_typ('HTML', EMPTY_BLOB()));

INSERT INTO TABLE(SELECT ad_textdocs_ntab FROM print_media WHERE ad_id = 12001)
VALUES (textdoc_typ('WORD', EMPTY_BLOB()));

INSERT INTO TABLE(SELECT ad_textdocs_ntab FROM print_media WHERE ad_id = 12002)
VALUES (textdoc_typ('PDF', EMPTY_BLOB()));

INSERT INTO TABLE(SELECT ad_textdocs_ntab FROM print_media WHERE ad_id = 12002)
VALUES (textdoc_typ('RTF', EMPTY_BLOB()));

INSERT INTO TABLE(SELECT ad_textdocs_ntab FROM print_media WHERE ad_id = 12003)
VALUES (textdoc_typ('PDF', EMPTY_BLOB()));

COMMIT;

-- Media files with various LOB data
INSERT INTO media_files (file_id, file_name, file_type, file_content, file_description, file_notes_ntl)
VALUES (
    1,
    'company_logo.png',
    'image/png',
    EMPTY_BLOB(),
    'Official company logo in PNG format. High resolution version for print media.',
    N'会社のロゴ。印刷メディア用の高解像度バージョン。'
);

INSERT INTO media_files (file_id, file_name, file_type, file_content, file_description, file_notes_ntl)
VALUES (
    2,
    'product_brochure.pdf',
    'application/pdf',
    EMPTY_BLOB(),
    'Product brochure containing all monitor specifications and pricing information.',
    N'すべてのモニター仕様と価格情報を含む製品パンフレット。'
);

INSERT INTO media_files (file_id, file_name, file_type, file_content, file_description, file_notes_ntl)
VALUES (
    3,
    'warranty_terms.docx',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    EMPTY_BLOB(),
    'Warranty terms and conditions document for all monitor products.',
    N'すべてのモニター製品の保証条件文書。'
);

COMMIT;

-- Product catalog with XMLType data
INSERT INTO product_catalog (catalog_id, catalog_name, catalog_xml)
VALUES (
    1,
    'Monitors 2024 Q1',
    XMLTYPE('<?xml version="1.0" encoding="UTF-8"?>
<catalog>
  <name>Monitor Product Catalog Q1 2024</name>
  <products>
    <product id="1726">
      <name>LCD Monitor 11/PM</name>
      <price currency="USD">259.00</price>
      <category>Desktop Monitors</category>
      <specs>
        <screen_size unit="inch">11</screen_size>
        <resolution>1280x720</resolution>
        <panel_type>PM-IPS</panel_type>
      </specs>
    </product>
    <product id="2359">
      <name>LCD Monitor 15/PM</name>
      <price currency="USD">459.00</price>
      <category>Desktop Monitors</category>
      <specs>
        <screen_size unit="inch">15</screen_size>
        <resolution>1920x1080</resolution>
        <panel_type>PM-IPS</panel_type>
      </specs>
    </product>
  </products>
</catalog>')
);

INSERT INTO product_catalog (catalog_id, catalog_name, catalog_xml)
VALUES (
    2,
    'Monitors 2024 Q2',
    XMLTYPE('<?xml version="1.0" encoding="UTF-8"?>
<catalog>
  <name>Monitor Product Catalog Q2 2024</name>
  <products>
    <product id="3060">
      <name>Monitor 17/HR</name>
      <price currency="USD">299.00</price>
      <category>Professional Monitors</category>
      <specs>
        <screen_size unit="inch">17</screen_size>
        <resolution>1920x1080</resolution>
        <panel_type>IPS</panel_type>
      </specs>
    </product>
    <product id="3106">
      <name>Monitor 19/SD</name>
      <price currency="USD">399.00</price>
      <category>Professional Monitors</category>
      <specs>
        <screen_size unit="inch">19</screen_size>
        <resolution>1920x1200</resolution>
        <panel_type>IPS</panel_type>
      </specs>
    </product>
  </products>
</catalog>')
);

COMMIT;

-- Update LOB columns with actual binary data (sample PNG header bytes as hex)
-- In production, you would load actual files using DBMS_LOB or external tools

DECLARE
    v_blob BLOB;
    v_raw RAW(2000);
BEGIN
    -- Sample PNG file header (first 100 bytes of a minimal PNG)
    v_raw := HEXTORAW('89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001849444154789C6360A01C3232B26432F20C0C0C6460A0000018310203CD2C69380000000049454E44AE426082');

    -- Update print_media BLOB columns
    FOR rec IN (SELECT product_id, ad_id FROM print_media) LOOP
        UPDATE print_media
        SET ad_composite = v_raw,
            ad_photo = v_raw
        WHERE product_id = rec.product_id AND ad_id = rec.ad_id;

        -- Update object type BLOB
        UPDATE print_media p
        SET ad_header.logo = v_raw
        WHERE product_id = rec.product_id AND ad_id = rec.ad_id;
    END LOOP;

    -- Update media_files BLOB column
    UPDATE media_files SET file_content = v_raw;

    -- Update nested table BLOBs
    UPDATE textdocs_nestedtab SET formatted_doc = v_raw;

    COMMIT;
END;
/

-- Verify data
SELECT 'print_media' as table_name, COUNT(*) as row_count FROM print_media
UNION ALL
SELECT 'media_files', COUNT(*) FROM media_files
UNION ALL
SELECT 'product_catalog', COUNT(*) FROM product_catalog
UNION ALL
SELECT 'product_information', COUNT(*) FROM product_information
UNION ALL
SELECT 'textdocs_nestedtab', COUNT(*) FROM textdocs_nestedtab;
