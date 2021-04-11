package br.com.qyon.atualizadorPostgres;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * Hello world!
 *
 */
public class atualizador {

	private static final String schema = "atualizador_postgres";
	private static final String tabela = "atualizado";

	public static void main(String[] args) throws IOException {
		// Busca
		File migration = new File("./migration");

		if (!migration.exists() || !migration.isDirectory())
			migration.mkdir();
		// throw new RuntimeException(
		// "É necessario criar um folder chamado migration, e colocar todos os arquivos
		// \".sql\" dentro dele");

		if (args.length != 4)
			throw new RuntimeException(
					"Por favor coloque as seguintes propriedades na sequencia: [url de conexao BD SEM NENHUMA BASE] [usuario do BD] [senha do BD] [pesquisa dos bancos de dados, usando like]");

		if (!args[0].endsWith("/"))
			args[0] += "/";

		String dbInicial = args[0];
		String usuario = args[1];
		String senha = args[2];
		String inicioDatabase = args[3];

		try (Connection connection = DriverManager.getConnection(dbInicial, usuario, senha)) {
			System.out.println("Connected to PostgreSQL database!");
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(
					"select datname from pg_catalog.pg_database pd where datname like '" + inicioDatabase + "'");
			System.out.println("Lendo os bancos de dados com like: " + inicioDatabase);
			while (resultSet.next()) {
				String banco = resultSet.getString("datname");
				System.out.println("Connecting to database: " + banco);
				try (Connection connection2 = DriverManager.getConnection(dbInicial + banco, usuario, senha)) {
					Statement statement2 = connection2.createStatement();

					// Ordena por nome todos os arquivos dentro do folder
					List<File> sqls = Arrays.asList(migration.listFiles());
					sqls.sort((o1, o2) -> o1.getName().compareTo(o2.getName()));

					for (File sql : sqls) {
						if (!sql.getName().endsWith(".sql"))
							continue;
						// Cria uma table que garante que não irá rodar o mesmo arquivo mais de uma vez
						statement2.execute("Create schema if not exists " + schema + ";\r\n" //
								+ " create table IF NOT EXISTS " + schema + "." + tabela
								+ "(\"id\" SERIAL PRIMARY KEY, \"script\" varchar not null, \"data\" timestamp not null, constraint \"unique_script\" unique (\"script\"));");

						String script = sql.getName();
						ResultSet resultSet2 = statement2.executeQuery(
								"select script from " + schema + "." + tabela + " where script like '" + script + "'");

						if (resultSet2.next()) {
							// Pula para o próximo arquivo
							continue;
						}

						System.out.println("Atualizando o script " + script);

						try (BufferedReader reader = new BufferedReader(new FileReader(sql));) {
							String currentLine;
							StringBuilder sb = new StringBuilder();

							while ((currentLine = reader.readLine()) != null) {
								sb.append(currentLine + "\r\n");
							}
							System.out.println(sb.toString());
							statement2.execute(sb.toString());

							// TODO salvar o nome do arquivo salvo
							statement2.execute("insert into " + schema + "." + tabela + "(script, data) values ('"
									+ script + "', current_timestamp)");
						}
					}

				}
			}

		} catch (SQLException e) {
			System.out.println("Ocorreu um problema durante a atualização.");
			e.printStackTrace();
		}
	}

}
