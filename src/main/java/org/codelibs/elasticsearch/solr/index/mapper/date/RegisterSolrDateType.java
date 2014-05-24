package org.codelibs.elasticsearch.solr.index.mapper.date;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.settings.IndexSettings;

public class RegisterSolrDateType extends AbstractIndexComponent {

    @Inject
    public RegisterSolrDateType(final Index index,
            @IndexSettings final Settings indexSettings,
            final MapperService mapperService) {
        super(index, indexSettings);

        mapperService.documentMapperParser().putTypeParser(
                SolrDateFieldMapper.CONTENT_TYPE,
                new SolrDateFieldMapper.TypeParser());
    }
}
