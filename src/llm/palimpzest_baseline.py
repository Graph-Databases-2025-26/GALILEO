from langchain_community.document_loaders import TextLoader
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_community.vectorstores import FAISS
from langchain_text_splitters import RecursiveCharacterTextSplitter

from config import Config_Loader
from src import LOG
from src.utils.constants import *


class MarkdownRAGBackend:
    def __init__(self, dataset_path: str, model_name="sentence-transformers/all-MiniLM-L6-v2"):
        """
        Backend for indexing Markdown or text files and retrieving relevant segments.
        :param dataset_path: dataset folder (e.g., “data/RAG/PREMIER” or “data/RAG/FORTUNE”)
        :param model_name: HuggingFace model for embeddings
        """
        self.dataset_path = Path(dataset_path)
        self.model_name = model_name
        self.embeddings = HuggingFaceEmbeddings(model_name=model_name, model_kwargs={"device": "cpu"})
        self.vectorstore = None

    def load_and_index(self):
        """Load file .md and files without extensions, create chunk and build FAISS index. """
        LOG.info(f" Loading file from: {self.dataset_path}")

        #find all the files .md or textual files without extensions
        file_paths = [
            f for f in self.dataset_path.iterdir()
            if f.is_file() and (f.suffix == ".md" or f.suffix == "" or f.suffix == ".txt")
        ]

        if not file_paths:
            raise FileNotFoundError("No .md or .txt or any text files found")

        LOG.info(f" -> {len(file_paths)} files found")

        #Load all the files as LangChain docs
        documents = []
        for file_path in file_paths:
            loader = TextLoader(str(file_path), encoding="utf-8")
            documents.extend(loader.load())

        # Set the chunks dimension based on dataset
        dataset_name = self.dataset_path.name.lower()
        chunk_size = 128 if "premier" in dataset_name else 400
        print(f"🔪Chunks splitting in {chunk_size} token...")

        splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=20)
        docs = splitter.split_documents(documents)
        print(f"   → {len(docs)} generated segments")

        print(f"Embedding generation with {self.model_name}...")
        self.vectorstore = FAISS.from_documents(docs, self.embeddings)
        print(" Indixing completed!")

    def retrieve(self, query: str):

            config = Config_Loader().get_config()

            """Returns the top_k most relevant chunks for a given query."""
            if self.vectorstore is None:
                raise ValueError("Index has not been built yet. Call load_and_index() first.")

            top_k = config.top_k
            results = self.vectorstore.similarity_search(query, k=top_k)
            return results

    def save_index(self, path="faiss_index"):
            """Saves the FAISS index to disk."""
            if self.vectorstore:
                self.vectorstore.save_local(path)
                print(f" FAISS index saved to: {path}")
            else:
                print(" No index available to save.")

    def load_index(self, path="faiss_index"):
            """Loads a FAISS index previously saved to disk."""
            if not os.path.exists(path):
                raise FileNotFoundError(f"No FAISS index found at: {path}")
            self.vectorstore = FAISS.load_local(path, self.embeddings, allow_dangerous_deserialization=True)
            print(f" FAISS index loaded from: {path}")

def pz_context(input_d: dict, db_schema:str, rag_sources_path: str, prompt: str):
    """
    Function that builds the extra-context parameter that will be passed to the next chain's component (FULL PROMPT)
    """


    backend = MarkdownRAGBackend(dataset_path=rag_sources_path)
    backend.load_and_index()

    final_rag_context = backend.retrieve(prompt)
    #LOG.info(f"PROMPT PER BACKEND RETRIEVE: {prompt}")

    LOG.info("\n Top relevant chunks:")
    for i, doc in enumerate(final_rag_context[:5]):
        LOG.info(f"\n[{i + 1}] {doc.metadata.get('source', 'unknown')}")
        LOG.info(doc.page_content[:300], "...")

    output = {
        "schema_info": db_schema,
        "raw_data": final_rag_context,  # Textual RAG context (50 simulated chunks)
        "query": input_d["prompt"],
    }

    return output
