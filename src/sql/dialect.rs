use sqlparser::dialect::Dialect;

/// A SQL dialect that extends GenericDialect with ksqlDB keywords.
#[derive(Debug, Default)]
pub struct KsqlDialect;

impl Dialect for KsqlDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_ascii_alphabetic() || ch == '_' || ch == '#' || ch == '@'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_ascii_alphanumeric() || ch == '_' || ch == '#' || ch == '@' || ch == '$'
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }
}
