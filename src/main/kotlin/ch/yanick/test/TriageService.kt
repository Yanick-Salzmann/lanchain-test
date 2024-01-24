package ch.yanick.test

import dev.langchain4j.data.document.Document
import dev.langchain4j.data.document.Metadata
import dev.langchain4j.data.document.splitter.DocumentSplitters.recursive
import dev.langchain4j.data.segment.TextSegment
import dev.langchain4j.model.embedding.EmbeddingModel
import dev.langchain4j.retriever.EmbeddingStoreRetriever
import dev.langchain4j.retriever.Retriever
import dev.langchain4j.service.MemoryId
import dev.langchain4j.service.SystemMessage
import dev.langchain4j.service.UserMessage
import dev.langchain4j.store.embedding.EmbeddingStoreIngestor
import io.quarkiverse.langchain4j.RegisterAiService
import io.quarkiverse.langchain4j.RegisterAiService.BeanRetrieverSupplier
import io.quarkiverse.langchain4j.redis.RedisEmbeddingStore
import io.quarkus.runtime.Startup
import jakarta.enterprise.context.ApplicationScoped
import org.apache.commons.csv.CSVFormat
import java.io.InputStreamReader


@RegisterAiService(retrieverSupplier = BeanRetrieverSupplier::class)
interface TriageService {
    @SystemMessage(
        """You are MovieMuse, an AI answering questions about the top 100 movies from IMDB.
            Your response must be polite, use the same language as the question, and be relevant to the question.

            Introduce yourself with: "Hello, I'm MovieMuse, how can I help you?""""
    )
    fun chat(@MemoryId session: Any, @UserMessage question: String): String
}

@ApplicationScoped
class RAGEmbeddingService(store: RedisEmbeddingStore, model: EmbeddingModel) : Retriever<TextSegment> {
    private val retriever = EmbeddingStoreRetriever.from(store, model, 10)

    override fun findRelevant(query: String): MutableList<TextSegment> = retriever.findRelevant(query)
}

@ApplicationScoped
class CsvDataSupplier {
    fun loadDocuments(): List<Document> {
        val documents = mutableListOf<Document>()

        javaClass.getResourceAsStream("/movies.csv").use { stream ->
            stream ?: throw IllegalArgumentException("Could not find movies.csv")
            CSVFormat.DEFAULT.builder()
                .setSkipHeaderRecord(true)
                .build().let {
                    it.parse(InputStreamReader(stream)).let { parser ->
                        val headers = listOf("index", "movie_name", "year_of_release", "category", "run_time", "genre", "imdb_rating", "votes", "gross_total")
                        parser.records
                            .drop(1)
                            .forEachIndexed { idx, record ->
                                val bldr = StringBuilder()
                                val metadata = mutableMapOf<String, String>()
                                metadata["source"] = "movies.csv"
                                metadata["row"] = (idx + 1).toString()
                                headers.forEachIndexed { idx, header ->
                                    metadata[header] = record[idx]
                                    bldr.append("$header:${record[idx]}").append("\n")
                                }

                                documents.add(Document(bldr.toString(), Metadata.from(metadata)))
                            }
                    }
                }

            return documents
        }
    }

    @Startup
    @ApplicationScoped
    class EagerAppBean(
        service: TriageService,
        embeddingModel: EmbeddingModel,
        embeddingStore: RedisEmbeddingStore,
        csvDataSupplier: CsvDataSupplier
    ) {
        init {
            EmbeddingStoreIngestor.builder()
                .embeddingModel(embeddingModel)
                .embeddingStore(embeddingStore)
                .documentSplitter(recursive(500, 0))
                .build()
                .ingest(csvDataSupplier.loadDocuments())

            val response = service.chat("session", "What is the highest grossing action movie in the last 15 years?")
            println(response)
        }
    }
}