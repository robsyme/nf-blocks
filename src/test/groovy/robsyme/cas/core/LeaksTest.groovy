package robsyme.cas.core

import spock.lang.Specification

/**
 * DESIGN.md §6: which names name a secret, judged segment by segment, and the
 * credential-free form of a URI that an InputSet name is cut from (§22). The
 * same names go through a RunManifest in {@link ScrubTest} and
 * {@link RunManifestAuditTest}; this pins the classifier on its own.
 */
class LeaksTest extends Specification {

    def 'a name holding a secret word as a whole segment is secret-named (#name)'() {
        expect:
        Leaks.isSecretKey(name)

        where:
        name << [
            'key', 'api_key', 'apiKey', 'ssh-key', 'API_KEY', 'AWS_SECRET_ACCESS_KEY', 'azure.storage.accountKey',
            'secret', 'clientSecret', 'github_token', 'accessToken', 'tower.accessToken', 'db_password', 'keyStorePassword',
            'passwd', 'credentials', 'GITHUB_PAT', 'basicAuth', 'env.API_AUTH',
            // P4 L1: pass, pwd and passphrase are secret words; Authorization is one too
            'DB_PASS', 'MYSQL_PWD', 'passphrase', 'ssh_passphrase', 'proxyPass', 'pass', 'pwd', 'Authorization', 'Proxy-Authorization',
            // plurals, as a list of secrets is written
            'tokens', 'keys', 'passwords', 'secrets', 'api_keys', 'github_tokens',
            // one lower-case word that ends in a secret word, or a known compound of key
            'apikey', 'APIKEY', 'accesstoken', 'dbpassword', 'clientsecret', 'sshkey', 'privatekey', 'secretkey', 'APIKey',
            // a key id is named after the key, and goes with it
            'private_key_id',
            // R2: one lower-case word, a database or login tool's name and pass or pwd
            'PGPASS', 'pgpass', 'env.PGPASS', 'SSHPASS', 'MYSQLPWD', 'dbpass', 'rootpwd', 'ADMINPASS', 'LDAPPASS',
            // B1: a read, only or filter word before pass does not qualify it; secrets beat false positives
            'only_pass', 'filter_pass', 'DB_READ_PASS', 'READ_ONLY_PASS', 'db_read_pass', 'FILTER_PASS', 'read_pass', 'filtered_pass',
        ]
    }

    def 'a name that only holds a secret word inside another word, or qualifies one, is not secret-named (#name)'() {
        expect:
        !Leaks.isSecretKey(name)

        where:
        name << [
            // a secret word inside another word
            'monkey_reads', 'keystone', 'turkey', 'keyword', 'hotkey', 'whiskey', 'tokenizer', 'secretome_db', 'secretory',
            'passwordless', 'bypass_qc', 'passage', 'compass', 'passes', 'author', 'manifest.authors', 'path', 'pattern',
            // a count, a sort order or a switch: the word is qualified
            'keep_tokens', 'max_tokens', 'maxTokens', 'num_tokens', 'sort_keys', 'sort_key', 'join_key', 'primary_key',
            'skip_auth', 'no_auth', 'low_pass', 'high_pass', 'two_pass', 'band_pass',
            // where a secret is kept, not the secret: a file, a path, a directory
            'keyfile', 'key_file', 'keyFile', 'password_file_name', 'ssh_key_path', 'token_dir', 'secrets_dir',
            // R2: pass next to a data or filter word is a QC verdict (MinKNOW's fastq_pass, a VCF's PASS)
            // B1: a read, only or filter word counts after pass only
            'pass_filter', 'vcf_pass_only', 'pass_only', 'fastq_pass', 'fastqPass', 'FASTQ_PASS',
            'bam_pass', 'pod5_pass', 'fast5_pass', 'fastq_fail', 'pass_reads', 'pass_fail', 'qc_pass', 'fail_pass',
            // R2: pass or pwd at the end of a word that names no database or login tool
            'compass', 'bypass', 'overpass', 'oldpwd',
        ]
    }

    def 'a name ends in a secret word by its last segment alone; plurals and qualified words do not (#name: #expected)'() {
        expect:
        Leaks.endsInSecretWord(name) == expected

        where:
        name               | expected
        'password'         | true
        'api-key'          | true
        'apiKey'           | true
        'passphrase'       | true
        'db-pass'          | true
        'mysql-pwd'        | true
        'pwd'              | true
        'x-api-key'        | true
        'X-Auth-Token'     | true
        'Authorization'    | true
        'apikey'           | true
        'accesstoken'      | true
        'PGPASS'           | true
        'sshpass'          | true
        'MYSQLPWD'         | true
        'sort-keys'        | false
        'max-tokens'       | false
        'keys'             | false
        'tokens'           | false
        'token-col'        | false
        'keyfile'          | false
        'password-file'    | false
        'skip-auth'        | false
        'low-pass'         | false
        'bypass'           | false
        'monkey'           | false
        'compass'          | false
        'fastq-pass'       | false
        'bam_pass'         | false
        'pass-filter'      | false
        'only-pass'        | true
        'read-pass'        | true
        'pass-reads'       | false
        'overpass'         | false
    }

    def 'a value under a secret name is kept when it is a plain boolean or number, a marker, a whole location, or a nucleotide word under pat alone (#name = #value: #kept)'() {
        expect:
        Leaks.keepsValue(name, value) == kept

        where:
        name          | value                 | kept
        'password'    | true                  | true
        'password'    | 8                     | true
        'password'    | 0.05d                 | true
        'filter_pass' | 'true'                | true
        'filter_pass' | "'False'"             | true
        'token'       | '0'                   | true
        'token'       | '-1'                  | true
        'token'       | '+3'                  | true
        'token'       | '1e-5'                | true
        'token'       | '.5'                  | true
        'token'       | '[secret]'            | true
        'token'       | '[redacted-location]' | true
        'PWD'         | '/home/x'             | true
        'db_password' | 's3://bucket/x'       | true
        'pat'         | 'GATC'                | true
        'params.pat'  | "'gatc'"              | true
        '--pat'       | 'RYKM'                | true
        'token'       | 'hunter2'             | false
        'token'       | '1.2.3'               | false
        'token'       | '0x1f'                | false
        'token'       | 'truely'              | false
        'token'       | ''                    | false
        'password'    | '/x:hunter2'          | false
        'password'    | '/x hunter2'          | false
        'GITHUB_PAT'  | 'GATTACA'             | false
        'adapter-pat' | 'RYKM'                | false
        'pat'         | 'abc123'              | false
        'pat'         | null                  | false
        // Secrets beat false positives: a whole number of more than three digits can be a PIN or
        // a numeric password, so under a secret name it is no plain value.
        'password'    | 12345678              | false
        'password'    | 12345678L             | false
        'password'    | '12345678'            | false
        'apiKey'      | 12345                 | false
        'pin'         | '1234'                | false
        'password'    | '-1234'               | false
        'pass'        | 100                   | true
        'pass'        | '999'                 | true
        'filter_pass' | '0.0005'              | true
        'token'       | 2.5e9d                | false   // integral and 1000 or more: a whole number (S7)
        'token'       | 2.5e-9d               | true
    }

    def 'withoutCredentials keeps a URI\'s scheme, host and path and drops its userinfo, query and fragment (#uri)'() {
        expect:
        Leaks.withoutCredentials(uri) == plain

        where:
        uri                                                    | plain
        'https://user:pass@host.example.org:8443/a/b.fq?x=1#f' | 'https://host.example.org:8443/a/b.fq'
        'https://host.example.org/a/b.fq?X-Amz-Signature=abc'  | 'https://host.example.org/a/b.fq'
        'https://acct.blob.core.windows.net/c/x?sv=1&sig=abc'  | 'https://acct.blob.core.windows.net/c/x'
        'ftp://a@b:c@host/x'                                   | 'ftp://host/x'
        'https://user:pass@host.example.org'                   | 'https://host.example.org'
        's3://bucket/x.fq#frag'                                | 's3://bucket/x.fq'
        'cas://bafkreia/x'                                     | 'cas://bafkreia/x'
        '/data/x.fq'                                           | '/data/x.fq'
        'x.fq?y'                                               | 'x.fq?y'
    }
}
