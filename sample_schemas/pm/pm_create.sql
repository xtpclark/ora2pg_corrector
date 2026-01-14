-- PM (Product Media) Sample Schema - Standalone Version
-- Based on Oracle Sample Schemas, modified to remove OE dependency
-- Excellent for testing LOB migrations: BLOB, CLOB, NCLOB, BFILE, nested tables, object types

-- Create PM user (run as SYS or SYSTEM)
-- CREATE USER pm IDENTIFIED BY pm123;
-- GRANT CONNECT, RESOURCE, UNLIMITED TABLESPACE TO pm;

-- Connect as PM user before running the rest

-- Object type for ad headers with BLOB logo
CREATE TYPE adheader_typ AS OBJECT (
    header_name    VARCHAR2(256),
    creation_date  DATE,
    header_text    VARCHAR2(1024),
    logo           BLOB
);
/

-- Object type for text documents with BLOB content
CREATE TYPE textdoc_typ AS OBJECT (
    document_typ   VARCHAR2(32),
    formatted_doc  BLOB
);
/

-- Nested table type for multiple text documents
CREATE TYPE textdoc_tab AS TABLE OF textdoc_typ;
/

-- Products table (standalone replacement for oe.product_information)
CREATE TABLE product_information (
    product_id          NUMBER(6) PRIMARY KEY,
    product_name        VARCHAR2(50),
    product_description VARCHAR2(2000),
    category_id         NUMBER(2),
    weight_class        NUMBER(1),
    warranty_period     INTERVAL YEAR(2) TO MONTH,
    supplier_id         NUMBER(6),
    product_status      VARCHAR2(20),
    list_price          NUMBER(8,2),
    min_price           NUMBER(8,2),
    catalog_url         VARCHAR2(50)
);

-- Main print_media table with all LOB types
CREATE TABLE print_media (
    product_id        NUMBER(6),
    ad_id             NUMBER(6),
    ad_composite      BLOB,           -- BLOB: composite advertisement image
    ad_sourcetext     CLOB,           -- CLOB: source text for the ad
    ad_finaltext      CLOB,           -- CLOB: final formatted text
    ad_fltextn        NCLOB,          -- NCLOB: national character text
    ad_textdocs_ntab  textdoc_tab,    -- Nested table of document BLOBs
    ad_photo          BLOB,           -- BLOB: photograph
    ad_graphic        BFILE,          -- BFILE: external file reference
    ad_header         adheader_typ,   -- Object type with BLOB
    CONSTRAINT printmedia_pk PRIMARY KEY (product_id, ad_id),
    CONSTRAINT printmedia_fk FOREIGN KEY (product_id)
        REFERENCES product_information(product_id)
) NESTED TABLE ad_textdocs_ntab STORE AS textdocs_nestedtab;

-- Additional table for testing standalone LOB columns
CREATE TABLE media_files (
    file_id           NUMBER(6) PRIMARY KEY,
    file_name         VARCHAR2(256) NOT NULL,
    file_type         VARCHAR2(50),
    file_content      BLOB,
    file_description  CLOB,
    file_notes_ntl    NCLOB,
    external_file     BFILE,
    created_date      DATE DEFAULT SYSDATE,
    modified_date     TIMESTAMP DEFAULT SYSTIMESTAMP
);

-- Table with XMLType for XML LOB testing
CREATE TABLE product_catalog (
    catalog_id        NUMBER(6) PRIMARY KEY,
    catalog_name      VARCHAR2(100),
    catalog_xml       XMLTYPE,
    last_updated      TIMESTAMP DEFAULT SYSTIMESTAMP
);

-- Index on nested table
CREATE INDEX textdocs_ntab_idx ON textdocs_nestedtab(document_typ);

-- Comments
COMMENT ON TABLE print_media IS 'Print media advertisements with various LOB types';
COMMENT ON COLUMN print_media.ad_composite IS 'Composite BLOB image of the advertisement';
COMMENT ON COLUMN print_media.ad_sourcetext IS 'CLOB source text for the advertisement';
COMMENT ON COLUMN print_media.ad_fltextn IS 'NCLOB national character text version';
COMMENT ON COLUMN print_media.ad_graphic IS 'BFILE reference to external graphic file';
